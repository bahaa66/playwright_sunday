defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.UpdateNotificationsWorker do
  @moduledoc """
  Worker module that will be called by the Exq Job Queue to execute the
  UpdateNotifications work
  """
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  @page_size 2000

  def perform(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting(notification_setting_id),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(notification_setting.event_definition_id) do
      # 1) stop the pipeline and ensure that if finishes processing its messages in the pipeline
      # before starting the update of notifications
      pause_event_pipeline(event_definition)

      # 2) once the pipeline has been stopped and the finished processing start the pagination
      # of events and update of notifications
      NotificationsContext.get_page_of_notifications(
        %{
          filter: %{notification_setting_id: notification_setting_id},
          select: [:id]
        },
        page_size: @page_size
      )
      |> update_notifications(notification_setting)

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
          "UpdateNotificationsWorker failed for NotificationSettingId: #{notification_setting_id}. Error: #{
            inspect(error, pretty: true)
          }"
        )
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp pause_event_pipeline(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Stopping EventPipeline for #{event_definition.topic}"
    )

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
        "ensure_pipeline_drained/1 exceeded number of attempts. Moving forward with UpdateNotifications"
      )
    else
      case EventPipeline.pipeline_running?(event_definition_id) or
             not EventPipeline.pipeline_finished_processing?(event_definition_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still draining... waiting for pipeline to drain before running UpdateNotifications"
          )

          Process.sleep(1000)
          ensure_pipeline_drained(event_definition_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} drained"
          )
      end
    end
  end

  defp update_notifications(
         %{entries: notifications, page_number: page_number, total_pages: total_pages},
         %{tag_id: tag_id, id: id, title: ns_title, assigned_to: assigned_to} =
           notification_setting
       ) do
    notification_ids = Enum.map(notifications, fn n -> n.id end)

    NotificationsContext.update_notifcations(
      %{
        filter: %{notification_ids: notification_ids},
        select: [:id, :tag_id, :title, :assigned_to]
      },
      set: [tag_id: tag_id, title: ns_title, assigned_to: assigned_to]
    )

    if page_number >= total_pages do
      {:ok, :success}
    else
      NotificationsContext.get_page_of_notifications(
        %{
          filter: %{notification_setting_id: id},
          select: [:id]
        },
        page_number: page_number + 1,
        page_size: @page_size
      )
      |> update_notifications(notification_setting)
    end
  end
end
