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

  def insert_all_multi(multi \\ Multi.new()) do
    multi
    |> Multi.merge(fn %{insert_notifications: {_, notifications}} ->
      Multi.new()
      |> Multi.insert_all(
        :create_system_notifications,
        SystemNotification,
        build_system_notifications(notifications)
      )
    end)
  end

  def update_all_multi(multi \\ Multi.new()) do
    #just updating the sys_notificaitons from the query to have deleted title and msg

    multi
    |> Multi.merge(fn %{update_notifications: {_, updated_notifications}} ->
      Multi.new()
      |> update_system_notifications(updated_notifications)
    end)
  end

  defp update_system_notifications(multi\\ Multi.new, notifications) do
    if notifications != %{} do
      deleted_notification_ids = notifications |> Enum.reduce([], fn notification, acc ->
          if !is_nil(notification.deleted_at) do
            [notification.id | acc]
          else
            acc
          end
        end)

      if deleted_notification_ids != [] do
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
                ^deleted_notification_ids
              )
          )

        multi
        |> Multi.update_all(:update_system_notifications, query, set: [title: "Notification Retracted",
        message: "One of your notifications has been retracted and no longer relevant.",
        updated_at: DateTime.truncate(DateTime.utc_now(), :second)])
      else
        multi
      end
  else
    multi
  end
end


  defp build_system_notifications(notifications) when notifications != %{} do
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
end
