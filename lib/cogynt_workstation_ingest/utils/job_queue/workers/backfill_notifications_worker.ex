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

  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Notifications.{Notification, NotificationSetting}
  alias Models.Events.EventDefinition

  @page_size 2000
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]

  def perform(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting_by(%{id: notification_setting_id}),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(notification_setting.event_definition_id,
             preload_details: true
           ) do
      # First stop the pipeline and ensure that if finishes processing its messages in the pipeline
      # before starting the backfill of notifications
      stop_event_pipeline(event_definition)

      # Second once the pipeline has been stopped and the finished processing start the pagination
      # of events and backfill of notifications
      page =
        EventsContext.get_page_of_events(
          %{
            filter: %{event_definition_id: event_definition.id},
            select: [:id]
          },
          page_number: 1,
          page_size: @page_size,
          preload_details: true
        )

      process_page(page, event_definition, notification_setting)

      # Finally check to see if consumer should be started back up again
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
  defp stop_event_pipeline(event_definition) do
    CogyntLogger.info("#{__MODULE__}", "Stopping EventPipeline for #{event_definition.topic}")

    {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

    if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
      Redis.publish_async("ingest_channel", %{
        stop_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    end

    ensure_event_pipeline_stopped(event_definition.id)
  end

  defp start_event_pipeline(event_definition) do
    case is_job_queue_finished?(event_definition.id) do
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

  defp ensure_event_pipeline_stopped(event_definition_id) do
    case EventPipeline.event_pipeline_running?(event_definition_id) or
           not EventPipeline.event_pipeline_finished_processing?(event_definition_id) do
      true ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before running backfill of notifications"
        )

        Process.sleep(500)
        ensure_event_pipeline_stopped(event_definition_id)

      false ->
        {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition_id)

        cond do
          consumer_state.prev_status == ConsumerStatusTypeEnum.status()[:paused_and_finished] or
            consumer_state.prev_status == ConsumerStatusTypeEnum.status()[:paused_and_processing] or
            consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_finished] or
              consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
            CogyntLogger.info("#{__MODULE__}", "EventPipeline #{event_definition_id} Stopped")

          true ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before running backfill of notifications"
            )

            Process.sleep(500)
            ensure_event_pipeline_stopped(event_definition_id)
        end
    end
  end

  defp process_page(
         %{entries: entries, page_number: page_number, total_pages: total_pages},
         %{id: event_definition_id, event_definition_details: event_definition_details} =
           event_definition,
         notification_setting
       ) do
    case build_notifications(entries, event_definition_details, notification_setting)
         |> NotificationsContext.bulk_insert_notifications(
           returning: Notification.__schema__(:fields)
         ) do
      {_count, []} ->
        nil

      {_count, updated_notifications} ->
        # only want to publish the notifications that are not deleted
        %{true: publish_notifications} =
          NotificationsContext.remove_notification_virtual_fields(updated_notifications)
          |> Enum.group_by(fn n -> is_nil(n.deleted_at) end)

        Redis.publish_async("notification_settings_subscription", %{
          updated: notification_setting.id
        })

        SystemNotificationContext.bulk_insert_system_notifications(publish_notifications)
    end

    if page_number >= total_pages do
      {:ok, :success}
    else
      next_page =
        EventsContext.get_page_of_events(
          %{
            filter: %{event_definition_id: event_definition_id},
            select: [:id]
          },
          page_number: page_number + 1,
          page_size: @page_size,
          preload_details: true
        )

      process_page(next_page, event_definition, notification_setting)
    end
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

  defp is_job_queue_finished?(id) do
    try do
      Enum.reduce(["notifications"], true, fn prefix, acc ->
        queue_name = prefix <> "-" <> "#{id}"
        {:ok, count} = Exq.Api.queue_size(Exq.Api, queue_name)
        {:ok, processes} = Exq.Api.processes(Exq.Api)

        grouped =
          Enum.group_by(processes, fn process ->
            decoded_job = Jason.decode!(process.job, keys: :atoms)
            decoded_job.queue
          end)

        queue_processes = Map.get(grouped, queue_name, [])

        if count > 0 or Enum.count(queue_processes) > 1 do
          false
        else
          acc
        end
      end)
    rescue
      _ ->
        CogyntLogger.error("#{__MODULE__}", "is_job_queue_finished?/1 Failed.")
        true
    end
  end
end
