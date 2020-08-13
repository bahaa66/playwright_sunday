defmodule CogyntWorkstationIngest.Supervisors.TaskSupervisor do
  @moduledoc """
  Dynamic Supervisor for all CogyntWorkstationIngest modules that implement Task.
  """
  use DynamicSupervisor

  alias CogyntWorkstationIngest.Servers.{
    NotificationsTaskMonitor,
    EventDefinitionTaskMonitor,
    DrilldownTaskMonitor,
    DeploymentTaskMonitor
  }

  alias CogyntWorkstationIngest.Utils.Tasks.{
    BackfillNotificationsTask,
    UpdateNotificationSettingTask,
    DeleteEventDefinitionEventsTask,
    DeleteDrilldownDataTask,
    DeleteTopicDataTask,
    DeleteDeploymentDataTask
  }

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Based on the args passed in will start the module to execute the async task to execute the
  work defined within the module
  """
  def start_child(args) do
    Enum.each(args, fn
      {:backfill_notifications, notification_setting_id} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {BackfillNotificationsTask, notification_setting_id}
          )

        NotificationsTaskMonitor.monitor(pid, notification_setting_id)
        {:ok, pid}

      {:update_notification_setting, notification_setting_id} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {UpdateNotificationSettingTask, notification_setting_id}
          )

        NotificationsTaskMonitor.monitor(pid, notification_setting_id)
        {:ok, pid}

      {:delete_event_definition_events, event_definition_id} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteEventDefinitionEventsTask, event_definition_id}
          )

        EventDefinitionTaskMonitor.monitor(pid, event_definition_id)
        {:ok, pid}

      {:delete_drilldown_data, delete_drilldown_topics} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteDrilldownDataTask, delete_drilldown_topics}
          )

        DrilldownTaskMonitor.monitor(pid)
        {:ok, pid}

      {:delete_deployment_data, nil} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteDeploymentDataTask}
          )

        DeploymentTaskMonitor.monitor(pid)
        {:ok, pid}

      {:delete_topic_data,
       %{event_definition_ids: event_definition_ids, delete_topics: _delete_topics} = args} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteTopicDataTask, args}
          )

        EventDefinitionTaskMonitor.monitor(pid, event_definition_ids)
        {:ok, pid}

      _ ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Invalid args passed. Args: #{inspect(args, pretty: true)}"
        )
    end)
  end
end
