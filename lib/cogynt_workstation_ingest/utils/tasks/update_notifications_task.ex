defmodule CogyntWorkstationIngest.Utils.Tasks.UpdateNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the update_notifications work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  alias Models.Notifications.{Notification, NotificationSetting}

  @page_size 2000

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(notification_setting_id), do: update_notifications(notification_setting_id)

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp update_notifications(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting(notification_setting_id) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Running update notifications task for ID: #{notification_setting_id}"
      )

      page =
        NotificationsContext.get_page_of_notifications(
          %{filter: %{notification_setting_id: notification_setting_id}},
          page_size: @page_size,
          include_deleted: true
        )

      process_page(page, notification_setting)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Notification setting not found for ID: #{notification_setting_id}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "UpdateNotificationsTask failed for ID: #{notification_setting_id}. Error: #{
            inspect(error, pretty: true)
          }"
        )
    end
  end

  defp process_page(
         %{entries: entries, page_number: page_number, total_pages: total_pages},
         %{tag_id: tag_id, id: id, title: ns_title, assigned_to: assigned_to} =
           notification_setting
       ) do
    notification_ids = Enum.map(entries, fn e -> e.id end)

    case NotificationsContext.update_notifcations(
           %{
             filter: %{notification_ids: notification_ids},
             select: Notification.__schema__(:fields)
           },
           set: [tag_id: tag_id, title: ns_title, assigned_to: assigned_to]
         ) do
      {_count, []} ->
        nil

      {_count, updated_notifications} ->
        Redis.publish_async("notification_settings_subscription", %{
          updated: notification_setting.id
        })
    end

    if page_number >= total_pages do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished processing notifications for notification_setting #{id}"
      )
    else
      next_page =
        NotificationsContext.get_page_of_notifications(
          %{filter: %{notification_setting_id: id}},
          page_number: page_number + 1,
          page_size: @page_size,
          include_deleted: true
        )

      process_page(next_page, notification_setting)
    end

    {:ok, :success}
  end
end
