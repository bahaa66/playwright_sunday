defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  alias Models.System.{NotificationDetails, SystemNotificationDetails, SystemNotification}
  alias CogyntWorkstationIngest.Repo
  import Ecto.Query
  alias Ecto.Multi

  def insert_or_update_system_notifications(created_notifications) do
    created_notifications
    |> build_system_notifications()
    |> bulk_insert_system_notifications()
  end

  def bulk_insert_system_notifications(system_notifications) do
    Repo.insert_all(SystemNotification, system_notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :message, :title, :type, :details]
    )
  end

  def build_system_notifications(notifications) when notifications != %{} do
    Enum.reduce(notifications, [], fn notification, acc ->
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
  end

  def insert_all_multi(multi \\ Multi.new(), notifications, updated_notifications \\ %{})
      when notifications != %{} do
    multi
    |> Multi.insert_all(
      :create_system_notifications,
      SystemNotification,
      build_system_notifications(notifications)
    )
    |> update_all_multi(updated_notifications)
  end

  def update_all_multi(multi \\ Multi.new(), notifications) do
    # clean the data and remove the deleted system_notifications
    # since the notification_id wont change, the only update would be if it got deleted or assigned new user that we care about.
    if notifications != %{} do
      multi
      |> Multi.update_all(
        :update_system_notifications,
        SystemNotification,
        build_update_system_notifications(notifications)
      )
      |> hard_delete_multi_all(notifications)
    else
      multi
    end
  end

  defp hard_delete_multi_all(multi \\ Multi.new(), notifications) when notifications != %{} do
    deleted_notifications =
      notifications
      |> Enum.reduce([], fn notification, acc ->
        if !is_nil(notification.deleted_at) do
          [notification | acc]
        else
          acc
        end
      end)

    deleted_list = deleted_notifications |> Enum.map(fn notification -> notification.id end)

    if deleted_notifications != [] and deleted_list != [] do
      query =
        query =
        from(sn in Models.System.SystemNotification,
          where:
            fragment(
              """
              ?->?->>? = ANY(?)
              """,
              sn.details,
              ^"notification",
              ^"notification_id",
              ^deleted_list
            )
        )

      multi
      |> Multi.delete_all(:delete_all, query)
      |> Multi.insert_all(
        :create_delete_system_notifications,
        SystemNotification,
        build_delete_notifications(deleted_notifications)
      )
    end
  end

  defp build_update_system_notifications(notifications) when notifications != %{} do
    Enum.reduce(notifications, [], fn notification, acc ->
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
              updated_at: DateTime.truncate(DateTime.utc_now(), :second)
            }
          ]
      else
        acc
      end
    end)
  end

  defp build_delete_notifications(notifications) do
    Enum.reduce(notifications, [], fn notification, acc ->
      notif = %NotificationDetails{
        notification_id: notification.id
        # event_id: notification.event_id // the event_id is deleted not relevant to save it at this point.
      }

      sys_details = %SystemNotificationDetails{notification: notif}

      title = "Notification Retracted"
      message = "One of your notifications has been retracted and no longer relevant."
      now = DateTime.truncate(DateTime.utc_now(), :second)

      acc ++
        [
          %{
            title: title,
            message: message,
            type: :info,
            assigned_to: notification.assigned_to,
            details: sys_details,
            updated_at: now,
            created_at: now
          }
        ]
    end)
  end
end
