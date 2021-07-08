defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.BackfillNotificationsWorker do
  @moduledoc """
  Worker module that will be called by the Exq Job Queue to execute the
  BackfillNotification work
  """
  require Ecto.Query
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.System.SystemNotificationContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition

  @page_size 5000

  def perform(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting_by(%{id: notification_setting_id}),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(notification_setting.event_definition_id,
             preload_details: true
           ) do
      # First pause the pipeline and ensure that it finishes processing its messages
      # before starting the backfill of notifications
      pause_event_pipeline(event_definition)

      # Second once the pipeline has been stopped and the finished processing start the pagination
      # of events and backfill of notifications
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id},
          select: [:core_id, :created_at]
        },
        page_number: 1,
        page_size: @page_size
      )
      |> process_notifications_for_events(notification_setting.id, event_definition)

      # Finally check to see if consumer should be restarted
      start_event_pipeline(event_definition)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "NotificationSetting/EventDefinition not found for NotificationSettingId: #{
            notification_setting_id
          }."
        )
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp pause_event_pipeline(event_definition) do
    CogyntLogger.info("#{__MODULE__}", "Pausing EventPipeline for #{event_definition.topic}")

    {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

    if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
      Redis.publish_async("ingest_channel", %{
        stop_consumer_for_notification_tasks:
          EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    end

    ensure_pipeline_drained(event_definition.id)
  end

  defp start_event_pipeline(event_definition) do
    case ExqHelpers.queue_finished_processing?("notifications", event_definition.id) do
      true ->
        {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

        if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
          ConsumerStateManager.upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_state.prev_status,
            prev_status: consumer_state.status
          )

          if consumer_state.prev_status == ConsumerStatusTypeEnum.status()[:running] do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Starting EventPipeline for #{event_definition.topic}"
            )

            Redis.publish_async("ingest_channel", %{
              start_consumer:
                EventsContext.remove_event_definition_virtual_fields(event_definition)
            })
          end
        end

      _ ->
        nil
    end
  end

  defp ensure_pipeline_drained(event_definition_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_pipeline_drained/1 exceeded number of attempts. Moving forward with BackfillNotifications"
      )
    else
      case EventPipeline.pipeline_running?(event_definition_id) or
             not EventPipeline.pipeline_finished_processing?(event_definition_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still draining... waiting for it to finish draining before running NotificationBackfill"
          )

          Process.sleep(1000)
          ensure_pipeline_drained(event_definition_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} Drained"
          )
      end
    end
  end

  defp process_notifications_for_events(
         %{entries: events, page_number: page_number, total_pages: total_pages},
         notification_setting_id,
         event_definition
       ) do
    Enum.reduce(events, [], fn event, acc ->
      # 1) Fetch all valid_notification_settings for each event
      valid_notification_setting =
        NotificationsContext.fetch_valid_notification_settings(
          %{
            event_definition_id: event_definition.id,
            active: true
          },
          event.risk_score,
          event_definition
        )
        |> Enum.find(nil, fn ns -> ns.id == notification_setting_id end)

      # 2) Ensure that there is a ValidNotificationSetting AND that there was not already a Notification
      # created during Ingest for that notification_setting_id and build a list of notifications to
      # create for each
      notification =
        if !is_nil(valid_notification_setting) do
          if is_nil(
               NotificationsContext.get_notification_by(
                 core_id: event.core_id,
                 notification_setting_id: notification_setting_id
               )
             ) do
            now = DateTime.truncate(DateTime.utc_now(), :second)

            [
              %{
                title: valid_notification_setting.title,
                archived_at: nil,
                priority: 3,
                assigned_to: valid_notification_setting.assigned_to,
                dismissed_at: nil,
                core_id: event.core_id,
                tag_id: valid_notification_setting.tag_id,
                notification_setting_id: valid_notification_setting.id,
                created_at: now,
                updated_at: now
              }
            ]
          else
            []
          end
        else
          []
        end

      acc ++ notification
    end)
    |> NotificationsContext.upsert_all_notifications(
      returning: [
        :core_id,
        :tag_id,
        :id,
        :title,
        :notification_setting_id,
        :created_at,
        :updated_at,
        :assigned_to
      ],
      on_conflict:
        {:replace,
         [:tag_id, :title, :updated_at, :assigned_to, :dismissed_at, :priority, :archived_at]},
      conflict_target: [:core_id, :notification_setting_id]
    )
    |> case do
      {0, []} ->
        nil

      {_count, upserted_notifications} ->
        SystemNotificationContext.bulk_insert_system_notifications(upserted_notifications)
    end

    if page_number >= total_pages do
      {:ok, :success}
    else
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id},
          select: [:core_id, :created_at]
        },
        page_number: page_number + 1,
        page_size: @page_size
      )
      |> process_notifications_for_events(notification_setting_id, event_definition)
    end
  end
end
