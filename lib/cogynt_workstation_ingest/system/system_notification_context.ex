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
    sys_notifications =
      build_system_notifications(
        notifications,
        SystemNotificationTypeIds.BulkNotificationAssignment.value()
      )

    Repo.insert_all(SystemNotification, sys_notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :details],
      timeout: 60_000
    )
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

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

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

  defp build_system_notifications([], _type_id), do: []

  defp build_system_notifications(notifications, type_id) do
    notifications
    |> Enum.group_by(fn notification -> notification.assigned_to end)
    |> Enum.reduce([], fn
      {nil, _notifications}, acc ->
        acc

      {assigned_to, notifications}, acc ->
        if should_create_system_notification?(type_id, assigned_to) do
          Enum.reduce(notifications, acc, fn notification, acc_1 ->
            system_notification_details = %SystemNotificationDetails{
              notification_id: notification.id,
              core_id: notification.core_id
            }

            now = DateTime.truncate(DateTime.utc_now(), :second)

            acc_1 ++
              [
                %{
                  assigned_to: notification.assigned_to,
                  details: system_notification_details,
                  created_at: now,
                  updated_at: now,
                  system_notification_type_id: type_id,
                  custom: %{}
                }
              ]
          end)
        else
          acc
        end
    end)
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
