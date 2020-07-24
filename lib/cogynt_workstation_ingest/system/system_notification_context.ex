defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  alias Models.System.{NotificationDetails, SystemNotificationDetails, SystemNotification}
  alias CogyntWorkstationIngest.Repo
  import Ecto.Query

  # ------------------------------------------ #
  # --- System Notification Schema Nethods --- #
  # ------------------------------------------ #
  @doc """
  Will insert each SystemNotification record into the database
  ## Examples
      iex> bulk_insert_system_notifications(%{field: value})
      {:ok, %SystemNotification{}}
      iex> bulk_insert_system_notifications(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def bulk_insert_system_notifications(notifications) when is_list(notifications) do
    notifications =
      notifications
      |> build_system_notifications()

    Repo.insert_all(SystemNotification, notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :message, :title, :type, :details]
    )
  end

  @doc """
  Will update each SystemNotification record into the database
  ## Examples
      iex> bulk_update_system_notifications(%{field: value})
      {:ok, %SystemNotification{}}
      iex> bulk_update_system_notifications(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def bulk_update_system_notifications(notifications) do
    case Enum.empty?(notifications) do
      false ->
        deleted_notification_ids =
          notifications
          |> Enum.reduce([], fn notification, acc ->
            if !is_nil(notification.deleted_at) do
              [notification.id | acc]
            else
              acc
            end
          end)

        case Enum.empty?(deleted_notification_ids) do
          false ->
            query =
              from(sn in SystemNotification,
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

            Repo.update_all(query,
              set: [
                title: "Notification Retracted",
                message: "One of your notifications has been retracted and no longer relevant.",
                updated_at: DateTime.truncate(DateTime.utc_now(), :second)
              ]
            )

          true ->
            nil
        end

      true ->
        nil
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp build_system_notifications(notifications) do
    case Enum.empty?(notifications) do
      false ->
        Enum.reduce(notifications, [], fn notification, acc ->
          case !is_nil(notification.assigned_to) and is_nil(notification.deleted_at) do
            true ->
              notification_details = %NotificationDetails{
                notification_id: notification.id,
                event_id: notification.event_id
              }

              system_notification_details = %SystemNotificationDetails{
                notification: notification_details
              }

              title = "Notificaition Assignment."
              message = "#{Recase.to_title(notification.title)} is now assigned to you."

              acc ++
                [
                  %{
                    title: title,
                    message: message,
                    type: :info,
                    assigned_to: notification.assigned_to,
                    details: system_notification_details,
                    created_at: DateTime.truncate(DateTime.utc_now(), :second),
                    updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                  }
                ]

            false ->
              acc
          end
        end)

      true ->
        []
    end
  end
end
