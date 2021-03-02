defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  import Ecto.Query
  alias Models.Enums.SystemNotificationTypeIds

  alias Models.System.{
    SystemNotificationConfiguration,
    SystemNotificationDetails,
    SystemNotification
  }

  alias CogyntWorkstationIngest.Repo

  @insert_batch_size 65535

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
    # Postgresql protocol has a limit of maximum parameters (65535)
    build_system_notifications(
      notifications,
      SystemNotificationTypeIds.BulkNotificationAssignment.value()
    )
    |> Enum.chunk_every(@insert_batch_size)
    |> Enum.reduce({0, []}, fn rows, {acc_count, acc_sys_notifications} ->
      {count, result} =
        Repo.insert_all(SystemNotification, rows,
          returning: [:id, :created_at, :updated_at, :assigned_to, :details]
        )

      {acc_count ++ count, acc_sys_notifications ++ result}
    end)
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
            # Postgresql protocol has a limit of maximum parameters (65535)
            build_system_notifications(
              deleted_notifications,
              SystemNotificationTypeIds.BulkNotificationRetraction.value()
            )
            |> Enum.chunk_every(@insert_batch_size)
            |> Enum.reduce({0, []}, fn rows, {acc_count, acc_sys_notifications} ->
              {count, result} =
                Repo.insert_all(SystemNotification, rows,
                  returning: [:id, :created_at, :updated_at, :assigned_to, :details]
                )

              {acc_count ++ count, acc_sys_notifications ++ result}
            end)

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

  def system_notification_config_exists?(args) do
    Enum.reduce(
      args,
      from(c in SystemNotificationConfiguration),
      &parse_list_system_notification_configs_args(&2, &1)
    )
    |> Repo.exists?()
  end

  defp parse_list_system_notification_configs_args(query, {:filter, filter}) do
    Enum.reduce(filter, query, &parse_list_system_notification_configs_filters(&2, &1))
  end

  defp parse_list_system_notification_configs_filters(query, {:user_id, user_id}) do
    where(query, [c], c.user_id == ^user_id)
  end

  defp parse_list_system_notification_configs_filters(
         query,
         {:system_notification_type_id, system_notification_type_id}
       ) do
    where(query, [c], c.system_notification_type_id == ^system_notification_type_id)
  end

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

  # The presence of a system notification config for a user indicates that the user has
  # turned off the setting. This allows the default of new settings to be "on" for all
  # users.
  defp should_create_system_notification?(system_notification_type_id, assigned_to) do
    not system_notification_config_exists?(%{
      filter: %{
        user_id: assigned_to,
        system_notification_type_id: system_notification_type_id
      }
    })
  end
end
