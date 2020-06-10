defmodule CogyntWorkstationIngest.Servers.NotificationsTaskMonitor do
  @moduledoc """
    Module that monitors the the status of Notification Tasks.
    Will push the status to Cogynt-OTP via RPC call when the Tasks
    are created and shutdown
  """

  use GenServer
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Servers.ConsumerStateManager
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

  def is_processing?(notification_setting_id) do
    GenServer.call(__MODULE__, {:is_processing, notification_setting_id}, 10_000)
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
  def handle_call({:is_processing, notification_setting_id}, _from, state) do
    {:reply, Map.has_key?(state, notification_setting_id), state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    notification_setting_id = Map.get(state, pid)

    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    start_consumer_args =
      EventsContext.get_event_definition_for_startup(notification_setting.event_definition_id)

    %{status: status, topic: topic, prev_status: prev_status, nsid: nsid} =
      ConsumerStateManager.get_consumer_state(notification_setting.event_definition_id)

    nsid = List.delete(nsid, notification_setting_id)

    if Enum.empty?(nsid) do
      cond do
        prev_status == ConsumerStatusTypeEnum.status()[:running] ->
          ConsumerStateManager.update_consumer_state(notification_setting.event_definition_id,
            topic: topic,
            status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
          )

          ConsumerStateManager.manage_request(start_consumer_args)

        true ->
          ConsumerStateManager.update_consumer_state(notification_setting.event_definition_id,
            topic: topic,
            status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
          )
      end
    else
      ConsumerStateManager.update_consumer_state(notification_setting.event_definition_id,
        topic: topic,
        status: status,
        prev_status: prev_status,
        nsid: nsid
      )
    end

    CogyntClient.publish_notification_task_status(
      notification_setting_id,
      :finished
    )

    new_state = Map.drop(state, [pid, notification_setting_id])
    {:noreply, new_state}
  end
end
