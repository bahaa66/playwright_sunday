defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteNotificationsWorker do
  @moduledoc """
  Worker module that will be called by the Exq Job Queue to execute the
  DeleteNotifications work
  """
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.EventPipeline

  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  @page_size 2000

  def perform(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting(notification_setting_id),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(notification_setting.event_definition_id) do
      # First stop the pipeline and ensure that if finishes processing its messages in the pipeline
      # before starting the delete of notifications
      stop_event_pipeline(event_definition)

      # Second once the pipeline has been stopped and the finished processing start the pagination
      # of events and delete of notifications
      page =
        NotificationsContext.get_page_of_notifications(
          %{
            filter: %{notification_setting_id: notification_setting_id},
            select: [:id]
          },
          page_size: @page_size
        )

      process_page(page, notification_setting)

      # Finally check to see if consumer should be started back up again
      start_event_pipeline(event_definition)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "NotificationSetting/EventDefinition not found for NotificationSettingId: #{
            notification_setting_id
          }"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "DeleteNotificationsWorker failed for ID: #{notification_setting_id}. Error: #{
            inspect(error, pretty: true)
          }"
        )
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp stop_event_pipeline(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Stopping EventPipeline for #{event_definition.topic}"
    )

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
            prev_status: ConsumerStatusTypeEnum.status()[:running]
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
          "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before running delete of notifications"
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
         %{deleted_at: deleted_at} = notification_setting
       ) do
    notification_ids = Enum.map(entries, fn e -> e.id end)

    case NotificationsContext.update_notifcations(
           %{
             filter: %{notification_ids: notification_ids},
             select: [:id, :deleted_at]
           },
           set: [deleted_at: deleted_at]
         ) do
      {_count, []} ->
        nil

      {_count, _deleted_notifications} ->
        Redis.publish_async("notification_settings_subscription", %{
          updated: notification_setting.id
        })
    end

    if page_number >= total_pages do
      {:ok, :success}
    else
      next_page =
        NotificationsContext.get_page_of_notifications(
          %{
            filter: %{notification_setting_id: notification_setting.id},
            select: [:id]
          },
          page_number: page_number + 1,
          page_size: @page_size
        )

      process_page(next_page, notification_setting)
    end
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
