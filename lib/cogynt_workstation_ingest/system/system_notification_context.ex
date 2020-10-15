defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  alias Models.Enums.SystemNotificationTypeIds
  alias Models.System.{SystemNotificationDetails, SystemNotification}
  alias CogyntWorkstationIngest.Repo

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
      |> build_system_notifications(SystemNotificationTypeIds.BulkNotificationAssignment.value())

    Repo.insert_all(SystemNotification, notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :details]
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
        deleted_notifications =
          notifications
          |> Enum.reduce([], fn notification, acc ->
            if !is_nil(notification.deleted_at) do
              [notification | acc]
            else
              acc
            end
          end)

        case Enum.empty?(deleted_notifications) do
          false ->
            deleted_notifications =
              deleted_notifications
              |> build_system_notifications(
                SystemNotificationTypeIds.BulkNotificationRetraction.value()
              )

            Repo.insert_all(SystemNotification, deleted_notifications,
              returning: [:id, :created_at, :updated_at, :assigned_to, :details]
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

  defp build_system_notifications(notifications, type_id) do
    case Enum.empty?(notifications) do
      false ->
        Enum.reduce(notifications, [], fn notification, acc ->
          case !is_nil(notification.assigned_to) and is_nil(notification.deleted_at) do
            true ->
              system_notification_details = %SystemNotificationDetails{
                notification_id: notification.id,
                event_id: notification.event_id
              }

              acc ++
                [
                  %{
                    assigned_to: notification.assigned_to,
                    details: system_notification_details,
                    created_at: DateTime.truncate(DateTime.utc_now(), :second),
                    updated_at: DateTime.truncate(DateTime.utc_now(), :second),
                    system_notification_type_id: type_id,
                    custom: %{}
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
