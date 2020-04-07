defmodule CogyntWorkstationIngest.Utils.UpdateNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the update_notifications work as a
  async task.
  """
  use Task
  require Logger
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias Models.Notifications.NotificationSetting
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  @spec run(any) :: any
  def run(notification_setting_id) do
    Logger.info(
      "Update Notifications Task: Running update notifications task for ID: #{
        notification_setting_id
      }"
    )

    update_notifications(notification_setting_id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp update_notifications(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting(notification_setting_id) do
      NotificationsContext.update_notification_setting_notifications(
        notification_setting,
        fn updated_notifications ->
          CogyntClient.publish_notifications(updated_notifications)
        end
      )
    else
      nil ->
        Logger.info(
          "Update Notifications Task: Running update notifications task for ID: #{
            notification_setting_id
          }"
        )
    end
  end
end
