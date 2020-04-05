defmodule CogyntWorkstationIngest.Utils.BackfillNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the backfill_notifications work as a
  async task.
  """
  use Task
  require Logger
  import Ecto.Query
  alias Models.Notifications.{NotificationSetting, Notification}
  alias Models.Events.{Event, EventDefinition}
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient

  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @partial Application.get_env(:cogynt_workstation_ingest, :core_keys)[:partial]

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(id) do
    Logger.info("Backfill Notifications Task: Running backfill notifications task for ID: #{id}")

    backfill_notifications(id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp backfill_notifications(id) do
    # Grab the notification setting for the notification setting id given
    notification_setting = get_notification_setting!(id)

    # Grab the event definition that matches the notification setting and preload its details
    event_definition = get_event_definition!(notification_setting.event_definition_id)

    event_query =
      from(e in Event,
        where: e.event_definition_id == type(^event_definition.id, :binary_id),
        where: is_nil(e.deleted_at)
      )

    {:ok, notifications} =
      Repo.transaction(fn ->
        Repo.stream(event_query)
        |> Repo.stream_preload(10, :event_details)
        |> Stream.map(fn event ->
          with true <- publish_notification?(event.event_details),
               true <-
                 !is_nil(
                   Enum.find(event_definition.event_definition_details, fn d ->
                     d.field_name == notification_setting.title
                   end)
                 ),
               nil <-
                 get_notification_by(
                   event_id: event.id,
                   notification_setting_id: notification_setting.id
                 ) do
            %{
              event_id: event.id,
              user_id: notification_setting.user_id,
              tag_id: notification_setting.tag_id,
              title: notification_setting.title,
              notification_setting_id: notification_setting.id,
              created_at: DateTime.truncate(DateTime.utc_now(), :second),
              updated_at: DateTime.truncate(DateTime.utc_now(), :second)
            }
          else
            _ ->
              %{}
          end
        end)
        |> Enum.to_list()
      end)

    {_count, updated_notifications} =
      Repo.insert_all(Notification, notifications,
        returning: [
          :event_id,
          :user_id,
          :tag_id,
          :id,
          :title,
          :notification_setting_id,
          :created_at,
          :updated_at
        ]
      )

    # Send created_notifications to subscription_queue
    CogyntClient.publish_notifications(updated_notifications)
  end

  defp get_notification_setting!(id), do: Repo.get!(NotificationSetting, id)

  defp get_event_definition!(id) do
    Repo.get!(EventDefinition, id)
    |> Repo.preload(:event_definition_details)
  end

  defp get_notification_by(clauses), do: Repo.get_by(Notification, clauses)

  defp publish_notification?(event_details) do
    partial =
      Enum.find(event_details, fn detail ->
        detail.field_name == @partial and detail.field_value == "true"
      end)

    risk_score =
      Enum.find(event_details, fn detail ->
        detail.field_name == @risk_score and detail.field_value != nil and
          String.to_float(detail.field_value) > 0
      end)

    if partial == nil or risk_score != nil do
      true
    else
      false
    end
  end
end
