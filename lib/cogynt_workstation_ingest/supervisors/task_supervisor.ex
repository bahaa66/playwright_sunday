defmodule CogyntWorkstationIngest.Supervisors.TaskSupervisor do
  @moduledoc """
  Dynamic Supervisor for all CogyntWorkstationIngest modules that implement Task.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Servers.NotificationsTaskMonitor

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
            {CogyntWorkstationIngest.Utils.BackfillNotificationsTask, notification_setting_id}
          )

        NotificationsTaskMonitor.monitor(pid, notification_setting_id)
        {:ok, pid}

      {:update_notification_setting, notification_setting_id} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {CogyntWorkstationIngest.Utils.UpdateNotificationSettingTask, notification_setting_id}
          )

        NotificationsTaskMonitor.monitor(pid, notification_setting_id)
        {:ok, pid}

      _ ->
        CogyntLogger.warn("TaskSupervisor Error", "Invalid args passed. Args: #{inspect(args)}")
    end)
  end
end
