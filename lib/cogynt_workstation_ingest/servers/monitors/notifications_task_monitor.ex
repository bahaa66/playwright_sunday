defmodule CogyntWorkstationIngest.Servers.NotificationsTaskMonitor do
  @moduledoc """
    Module that monitors the the status of Notification Tasks.
    Will push the status to Cogynt-OTP via RPC call when the Tasks
    are created and shutdown
  """

  use GenServer
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid, notification_setting_id) do
    GenServer.cast(__MODULE__, {:monitor, pid, notification_setting_id})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid, notification_setting_id}, state) do
    Process.monitor(pid)

    new_state =
      Map.put(state, pid, notification_setting_id)
      |> Map.put(notification_setting_id, pid)

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # TODO implement retry for backfill/update task if reason anything other than :normal or :shutdown
    notification_setting_id = Map.get(state, pid)

    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    {:ok, consumer_state} =
      ConsumerStateManager.get_consumer_state(notification_setting.event_definition_id)

    nsid = List.delete(consumer_state.nsid, notification_setting_id)

    if Enum.empty?(nsid) do
      cond do
        consumer_state.prev_status == ConsumerStatusTypeEnum.status()[:running] ->
          ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
            topic: consumer_state.topic,
            nsid: nsid,
            status: ConsumerStatusTypeEnum.status()[:paused_and_finished],
            module: __MODULE__
          )

          EventsContext.get_event_definition_for_startup(notification_setting.event_definition_id)
          |> ConsumerStateManager.manage_request()

        true ->
          ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
            topic: consumer_state.topic,
            nsid: nsid,
            prev_status: ConsumerStatusTypeEnum.status()[:paused_and_processing],
            status: ConsumerStatusTypeEnum.status()[:paused_and_finished],
            module: __MODULE__
          )
      end
    else
      ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
        nsid: nsid,
        module: __MODULE__
      )
    end

    # TODO Redis pub/sub notification finished

    new_state = Map.drop(state, [pid, notification_setting_id])
    {:noreply, new_state}
  end
end
