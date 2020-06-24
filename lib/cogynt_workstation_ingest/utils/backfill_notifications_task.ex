defmodule CogyntWorkstationIngest.Utils.BackfillNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the backfill_notifications work as a
  async task.
  """
  use Task
  require Ecto.Query
  alias CogyntWorkstationIngest.Servers.Caches.NotificationSubscriptionCache
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.System.SystemNotificationContext

  @page_size 500
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(notification_setting_id) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running backfill notifications task for ID: #{notification_setting_id}"
    )

    backfill_notifications(notification_setting_id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp backfill_notifications(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting_by(%{id: notification_setting_id}),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition!(notification_setting.event_definition_id) do
      page =
        EventsContext.get_page_of_events(
          %{filter: %{event_definition_id: event_definition.id}},
          page_number: 1,
          page_size: @page_size
        )

      process_page(page, event_definition, notification_setting)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "NotificationSetting or EventDefinition not found for notification_setting_id: #{
            notification_setting_id
          }."
        )
    end
  end

  defp process_page(
         %{entries: entries, page_number: page_number, total_pages: total_pages},
         %{id: event_definition_id, event_definition_details: event_definition_details} =
           event_definition,
         notification_setting
       ) do
    {_count, updated_notifications} =
      build_notifications(entries, event_definition_details, notification_setting)
      |> NotificationsContext.bulk_insert_notifications(
        returning: [
          :event_id,
          :user_id,
          :tag_id,
          :id,
          :title,
          :notification_setting_id,
          :created_at,
          :updated_at,
          :assigned_to
        ]
      )

    SystemNotificationContext.insert_or_update_system_notifications(updated_notifications)
    NotificationSubscriptionCache.add_new_notifications(updated_notifications)

    case page_number >= total_pages do
      true ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Finished processing notifications for event_definition: #{event_definition_id} and notification_setting #{
            notification_setting.id
          }"
        )

      false ->
        next_page =
          EventsContext.get_page_of_events(
            %{filter: %{event_definition_id: event_definition_id}},
            page_number: page_number + 1,
            page_size: @page_size
          )

        process_page(next_page, event_definition, notification_setting)
    end

    {:ok, :success}
  end

  defp build_notifications(page_entries, event_definition_details, notification_setting) do
    Enum.reduce(page_entries, [], fn event, acc ->
      with true <- publish_notification?(event.event_details, notification_setting.risk_range),
           true <-
             !is_nil(
               Enum.find(event_definition_details, nil, fn d ->
                 d.field_name == notification_setting.title
               end)
             ),
           nil <-
             NotificationsContext.get_notification_by(
               event_id: event.id,
               notification_setting_id: notification_setting.id
             ) do
        acc ++
          [
            %{
              event_id: event.id,
              user_id: notification_setting.user_id,
              assigned_to: notification_setting.assigned_to,
              tag_id: notification_setting.tag_id,
              title: notification_setting.title,
              notification_setting_id: notification_setting.id,
              created_at: DateTime.truncate(DateTime.utc_now(), :second),
              updated_at: DateTime.truncate(DateTime.utc_now(), :second)
            }
          ]
      else
        _ ->
          acc
      end
    end)
  end

  defp publish_notification?(event_details, risk_range) do
    risk_score =
      Enum.find(event_details, 0, fn detail ->
        detail.field_name == @risk_score and detail.field_value != nil
      end)

    risk_score =
      if risk_score != 0 do
        case Float.parse(risk_score.field_value) do
          :error ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Failed to parse risk_score as a float. Defaulting to 0"
            )

            0

          {score, _extra} ->
            score
        end
      else
        risk_score
      end

    NotificationsContext.in_risk_range?(risk_score, risk_range)
  end
end
