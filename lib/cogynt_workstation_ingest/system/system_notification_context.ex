defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  import Ecto.Query
  alias Models.Enums.SystemNotificationTypeIds
  alias Models.System.{SystemNotificationConfiguration, SystemNotificationDetails, SystemNotification}
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

  @doc """
  Gets a single SystemNotificationConfiguration from the database
  ## Examples
      iex> get_system_notification_config_by(user_id: "d99f475b-1ad0-439b-958b-00f4af258995")
      %SystemNotificationConfiguration{}
      iex> get_system_notification_config_by(system_notification_type_id: 55)
      nil
  """
  def get_system_notification_config_by(clauses),
    do: Repo.get_by(from(c in SystemNotificationConfiguration), clauses)

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp build_system_notifications(notifications, type_id) do
    case Enum.empty?(notifications) do
      false ->
        Enum.group_by(notifications, &Map.get(&1, :assigned_to))
        |> Enum.reduce([], fn
          {nil, _notifications}, a ->
            a

          {user_id, notifications}, a ->
            if should_create_system_notification?(type_id, user_id) do
              Enum.reduce(notifications, a, fn notification, acc ->
                if is_nil(notification.deleted_at) do
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

                    else
                    acc
                end
              end)
            else
              a
            end
        end)
      true ->
        []
    end
  end

  defp should_create_system_notification?(system_notification_type_id, assigned_to) do
    get_system_notification_config_by(
      system_notification_type_id: system_notification_type_id,
      user_id: assigned_to
    )
  end
end
