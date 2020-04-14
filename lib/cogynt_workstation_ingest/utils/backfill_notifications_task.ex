defmodule CogyntWorkstationIngest.Utils.BackfillNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the backfill_notifications work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext

  @page_size 500
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @partial Application.get_env(:cogynt_workstation_ingest, :core_keys)[:partial]

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(id) do
    CogyntLogger.info(
      "Backfill Notifications Task",
      "Running backfill notifications task for ID: #{id}"
    )

    backfill_notifications(id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp backfill_notifications(id) do
    notification_setting = NotificationsContext.get_notification_setting!(id)

    event_definition =
      EventsContext.get_event_definition!(notification_setting.event_definition_id)

    page =
      EventsContext.paginate_events_by_event_definition_id(
        event_definition.id,
        1,
        @page_size
      )

    process_page(page, event_definition, notification_setting)
  end

  defp process_page(page, event_definition, notification_setting) do
    {:ok, updated_notifications} =
      build_notifications(page.entries, event_definition, notification_setting)
      |> NotificationsContext.bulk_insert_notifications()

    CogyntClient.publish_notifications(updated_notifications)

    case page.page_number == page.total_pages do
      true ->
        CogyntLogger.info(
          "Backfill Notifications",
          "Finished processing notifications for event_definition: #{event_definition.id} and notification_setting #{
            notification_setting.id
          }"
        )

      false ->
        new_page =
          EventsContext.paginate_events_by_event_definition_id(
            event_definition.id,
            page.page_number + 1,
            @page_size
          )

        process_page(new_page, event_definition, notification_setting)
    end

    {:ok, :success}
  end

  defp build_notifications(page_entries, event_definition, notification_setting) do
    Enum.reduce(page_entries, [], fn event, acc ->
      with true <- publish_notification?(event.event_details),
           true <-
             !is_nil(
               Enum.find(event_definition.event_definition_details, fn d ->
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

  defp publish_notification?(event_details) do
    partial =
      Enum.find(event_details, fn detail ->
        detail.field_name == @partial and detail.field_value == "true"
      end)

    risk_score =
      Enum.find(event_details, fn detail ->
        if detail.field_name == @risk_score and detail.field_value != nil do
          case Float.parse(detail.field_value) do
            :error ->
              nil

            {risk_score_val, _extra} ->
              risk_score_val > 0
          end
        else
          nil
        end
      end)

    if partial == nil or risk_score != nil do
      true
    else
      false
    end
  end
end
