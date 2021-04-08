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
  alias Models.Notifications.NotificationSetting
  alias Models.Events.EventDefinition

  @page_size 5000
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
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id},
          select: [:id, :core_id, :created_at]
        },
        page_number: 1,
        page_size: @page_size,
        preload_details: true
      )
      |> process_notifications_for_events(notification_setting.id, event_definition)

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
        stop_consumer_for_notification_tasks:
          EventsContext.remove_event_definition_virtual_fields(event_definition)
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

  defp ensure_event_pipeline_stopped(event_definition_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_event_pipeline_stopped/1 exceeded number of attempts. Moving forward with BackfillNotifications"
      )
    else
      case EventPipeline.event_pipeline_running?(event_definition_id) or
             not EventPipeline.event_pipeline_finished_processing?(event_definition_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before running backfill of notifications"
          )

          Process.sleep(1000)
          ensure_event_pipeline_stopped(event_definition_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} Stopped"
          )
      end
    end
  end

  defp process_notifications_for_events(
         %{entries: events, page_number: page_number, total_pages: total_pages},
         notification_setting_id,
         event_definition
       ) do
    notifications =
      Enum.reduce(events, [], fn event, acc ->
        risk_score = fetch_risk_score_from_event_details(event.event_details)

        # 1) Fetch all valid_notification_settings for each event
        valid_notification_setting =
          NotificationsContext.fetch_valid_notification_settings(
            %{
              event_definition_id: event_definition.id,
              deleted_at: nil,
              active: true
            },
            risk_score,
            event_definition
          )
          |> Enum.find(nil, fn ns -> ns.id == notification_setting_id end)

        # 2) Update all notifications that match invalid_notification_settings to be deleted
        invalid_notification_setting_ids =
          NotificationsContext.fetch_invalid_notification_settings(
            %{
              event_definition_id: event_definition.id,
              deleted_at: nil,
              active: true
            },
            risk_score,
            event_definition
          )
          |> Enum.map(fn ns -> ns.id end)

        if !Enum.empty?(invalid_notification_setting_ids) do
          NotificationsContext.update_notifcations(
            %{
              filter: %{
                notification_setting_ids: invalid_notification_setting_ids,
                deleted_at: nil,
                core_id: event.core_id
              }
            },
            set: [deleted_at: DateTime.truncate(DateTime.utc_now(), :second)]
          )
        end

        # 3) Ensure that there is a ValidNotificationSetting AND that there was not already a Notification
        # created during Ingest for that notification_setting_id and build a list of notifications to
        # create for each
        notification =
          if !is_nil(valid_notification_setting) do
            if is_nil(
                 NotificationsContext.get_notification_by(
                   event_id: event.id,
                   notification_setting_id: notification_setting_id
                 )
               ) do
              # Set fields for Stream_input
              id = Ecto.UUID.generate()
              title = valid_notification_setting.title || 'null'
              description = 'null'
              user_id = valid_notification_setting.user_id || 'null'
              archived_at = 'null'
              priority = 3
              assigned_to = valid_notification_setting.assigned_to || 'null'
              dismissed_at = 'null'
              deleted_at = 'null'
              core_id = event.core_id || 'null'
              now = DateTime.truncate(DateTime.utc_now(), :second)
              created_at = event.created_at || now

              [
                "#{id};#{title};#{description};#{user_id};#{archived_at};#{priority};#{
                  assigned_to
                };#{dismissed_at};#{deleted_at};#{event.id};#{valid_notification_setting.id};#{
                  valid_notification_setting.tag_id
                };#{core_id};#{created_at};#{now}\n"
              ]
            else
              []
            end
          else
            []
          end

        acc ++ notification
      end)

    # 4) Insert notifications for valid_notification_settings
    case NotificationsContext.insert_all_notifications_with_copy(notifications) do
      {:ok, %Postgrex.Result{rows: []}} ->
        nil

      {:ok, %Postgrex.Result{rows: results}} ->
        notifications_enum = NotificationsContext.map_postgres_results(results)

        # only want to publish the notifications that are not deleted
        %{true: publish_notifications} =
          Enum.group_by(notifications_enum, fn n -> is_nil(n.deleted_at) end)

        # Redis.publish_async("notification_settings_subscription", %{
        #   updated: notification_setting_id
        # })

        SystemNotificationContext.bulk_insert_system_notifications(publish_notifications)

      {:error, %Postgrex.Error{postgres: %{message: error}}} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "process_notifications failed with Error: #{inspect(error)}"
        )

      _ ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "process_notifications failed"
        )
    end

    if page_number >= total_pages do
      {:ok, :success}
    else
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id},
          select: [:id, :core_id, :created_at]
        },
        page_number: page_number + 1,
        page_size: @page_size,
        preload_details: true
      )
      |> process_notifications_for_events(notification_setting_id, event_definition)
    end
  end

  defp fetch_risk_score_from_event_details(event_details) do
    risk_score =
      Enum.find(event_details, 0, fn detail ->
        detail.field_name == @risk_score and detail.field_value != nil
      end)

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
