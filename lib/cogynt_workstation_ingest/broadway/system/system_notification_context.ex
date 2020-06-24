defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  alias Models.System.{NotificationDetails, SystemNotificationDetails, SystemNotification}
  alias CogyntWorkstationIngest.Repo

  def insert_or_update_system_notifications(created_notifications, updated_notifications \\ %{}) do
    # if created_notifications != %{} do
    #   created_notifications
    #   |> Enum.map(fn n -> n.id end)
    # end

    # if updated_notifications != %{} do
    #   updated_notifications
    #   |> Enum.map(fn n -> n.id end)
    # end

    # if(created_notifications != %{} and updated_notifications != %{}) do
    #   Map.merge(created_notifications, updated_notifications, fn v1, v2 ->
    #     if is_nil(v1.deleted_at) != nil and is_nil(v2.deleted_at)
    #   end)
    # end
    sys_notifications =
      Enum.reduce(created_notifications, [], fn notification, acc ->
        if(!is_nil(notification.assigned_to) and is_nil(notification.deleted_at)) do
          notif = %NotificationDetails{
            notification_id: notification.id,
            event_id: notification.event_id
          }

          sys_details = %SystemNotificationDetails{notification: notif}

          title = "Notification assigned"
          message = "You have been assigned a new notification"

          acc ++
            [
              %{
                title: title,
                message: message,
                type: :info,
                assigned_to: notification.assigned_to,
                details: sys_details,
                created_at: DateTime.truncate(DateTime.utc_now(), :second),
                updated_at: DateTime.truncate(DateTime.utc_now(), :second)
              }
            ]
        else
          acc
        end
      end)

    bulk_insert_system_notifications(sys_notifications)
  end

  def bulk_insert_system_notifications(system_notifications) do
    Repo.insert_all(SystemNotification, system_notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :message, :title, :type, :details]
    )
    |> IO.inspect()
  end
end
