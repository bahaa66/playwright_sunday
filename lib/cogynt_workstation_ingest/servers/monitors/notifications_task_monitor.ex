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
    GenServer.call(__MODULE__, {:is_processing, notification_setting_id})
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

    event_definition =
      EventsContext.get_event_definition(notification_setting.event_definition_id)

    # ConsumerStateManager.update_consumer_state(
    #   event_definition.id,
    #   event_definition.topic,
    #   ConsumerStatusTypeEnum.status()[:paused_and_finished],
    #   __MODULE__
    # )

    CogyntClient.publish_notification_task_status(
      notification_setting_id,
      :finished
    )

    new_state = Map.drop(state, [pid, notification_setting_id])
    {:noreply, new_state}
  end
end
