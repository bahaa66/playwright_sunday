defmodule CogyntWorkstationIngest.System.SystemNotificationContext do
  @moduledoc """
  The SystemNotificationContext: public interface for systen_notification related functionality.
  """

  alias Models.System.{NotificationDetails, SystemNotificationDetails, SystemNotification}
  alias CogyntWorkstationIngest.Repo
  import Ecto.Query
  alias Ecto.Multi

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
  def bulk_insert_system_notifications(system_notifications) when is_list(system_notifications) do
    system_notifications =
      system_notifications
      |> build_system_notifications()

    Repo.insert_all(SystemNotification, system_notifications,
      returning: [:id, :created_at, :updated_at, :assigned_to, :message, :title, :type, :details]
    )
  end

  # --------------------- #
  # --- Multi Methods --- #
  # --------------------- #
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
    # just updating the sys_notificaitons from the query to have deleted title and msg

    multi
    |> Multi.merge(fn %{update_notifications: {_, updated_notifications}} ->
      Multi.new()
      |> update_system_notifications(updated_notifications)
    end)
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

              title = "Notification assigned"
              message = "You have been assigned a new notification"

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

  defp update_system_notifications(multi \\ Multi.new(), notifications) do
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

            multi
            |> Multi.update_all(:update_system_notifications, query,
              set: [
                title: "Notification Retracted",
                message: "One of your notifications has been retracted and no longer relevant.",
                updated_at: DateTime.truncate(DateTime.utc_now(), :second)
              ]
            )

          true ->
            multi
        end

      true ->
        multi
    end
  end
end
