defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteNotificationsWorker do
  @moduledoc """
  Worker module that will be called by the Exq Job Queue to execute the
  DeleteNotifications work
  """
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Notes.Note

  alias CogyntWorkstationIngest.Repo
  import Ecto.Query, warn: false

  def perform(notification_setting_id) do
    with %NotificationSetting{} = notification_setting <-
           NotificationsContext.get_notification_setting(notification_setting_id),
         %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(notification_setting.event_definition_hash_id) do
      # 1) pause the pipeline and ensure that if finishes processing its messages
      # before starting the delete of notifications
      pause_event_pipeline(event_definition)

      # 2) fetch all notifications for the Notification Setting ID
      notification_ids =
        NotificationsContext.query_notifications(%{
          filter: %{
            notification_setting_id: notification_setting_id
          },
          select: [:id]
        })
        |> Enum.map(fn n -> n.id end)

      # 3) delete all Notes linked to the notifications removed for the notification_setting_id
      # TODO: move this to NoteContext module and remove reference to CogyntWorkstationIngest.Repo
      from(n in Note, where: n.parent_id in ^notification_ids)
      |> Repo.delete_all()

      # 4) delete all notifications linked to the notification_setting_id
      NotificationsContext.hard_delete_notifications(notification_setting_id)

      # 5) delete the notification setting
      NotificationsContext.hard_delete_notification_setting(notification_setting_id)

      # Finally check to see if consumer should be started back up again
      start_event_pipeline(event_definition)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "NotificationSetting/EventDefinition not found for NotificationSettingId: #{notification_setting_id}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "DeleteNotificationsWorker failed for ID: #{notification_setting_id}. Error: #{inspect(error, pretty: true)}"
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

  defp ensure_pipeline_drained(event_definition_hash_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_pipeline_drained/1 exceeded number of attempts. Moving forward with DeleteNotifications"
      )
    else
      case EventPipeline.pipeline_running?(event_definition_hash_id) or
             not EventPipeline.pipeline_finished_processing?(event_definition_hash_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_hash_id} still draining... waiting for pipeline to drain before running DeleteNotifications"
          )

          Process.sleep(1000)
          ensure_pipeline_drained(event_definition_hash_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_hash_id} drained"
          )
      end
    end
  end
end
