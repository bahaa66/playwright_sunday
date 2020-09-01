defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteEventDefinitionsAndTopicsTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Collections.CollectionsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.Producer
  alias CogyntWorkstationIngest.Servers.Caches.DeleteEventDefinitionDataCache

  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Notifications.NotificationSetting

  @page_size 2000

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(%{
        event_definition_ids: event_definition_ids,
        hard_delete: hard_delete_event_definitions,
        delete_topics: delete_topics
      }) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_topic_data_task for event_definition_ids: #{event_definition_ids}, hard_delete_event_definitions: #{
        hard_delete_event_definitions
      }, delete_topics: #{delete_topics}"
    )

    Enum.each(event_definition_ids, fn event_definition_id ->
      start_deleting(event_definition_id, hard_delete_event_definitions, delete_topics)
    end)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp start_deleting(event_definition_id, hard_delete_event_definitions, delete_topics) do
    case EventsContext.get_event_definition(event_definition_id) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Event definition not found for event_definition_id: #{event_definition_id}"
        )

      event_definition ->
        # First stop the consumer if there is one running for the event_definition
        CogyntLogger.info(
          "#{__MODULE__}",
          "Stoping ConsumerGroup for #{event_definition.topic}"
        )

        ConsumerStateManager.manage_request(%{stop_consumer: event_definition.id})

        # Second check to see if the topic needs to be deleted
        if delete_topics do
          CogyntLogger.info(
            "#{__MODULE__}",
            "Deleting Kakfa topic: #{event_definition.topic}"
          )

          worker_name = String.to_atom("deployment#{event_definition.deployment_id}")

          if Process.whereis(worker_name) != nil do
            KafkaEx.delete_topics([event_definition.topic], worker_name: worker_name)
          end
        end

        # Third flush any data that might be stored waiting to process
        Producer.flush_queue(event_definition.id)

        # Fourth check the consumer_state to make sure if it has any data left in the pipeline
        {:ok, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

        cond do
          consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
              consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
            cond do
              consumer_state.prev_status ==
                  ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
                # If data is still processing add event_definition_id to the cache to
                # trigger the delete task when it is done
                CogyntLogger.warn(
                  "#{__MODULE__}",
                  "Messages still processing. Will finish DevDelete when they are flushed from pipeline"
                )

                DeleteEventDefinitionDataCache.upsert_status(event_definition.id,
                  status: :waiting,
                  hard_delete: hard_delete_event_definitions
                )

              true ->
                delete_event_definition(event_definition, hard_delete_event_definitions)
            end

          consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
            # If data is still processing add event_definition_id to the cache to
            # trigger the delete task when it is done
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Messages still processing. Will finish DevDelete when they are flushed from pipeline"
            )

            DeleteEventDefinitionDataCache.upsert_status(event_definition.id,
              status: :waiting,
              hard_delete: hard_delete_event_definitions
            )

          true ->
            delete_event_definition(event_definition, hard_delete_event_definitions)
        end
    end
  end

  defp delete_event_definition(event_definition, hard_delete_event_definitions) do
    # delete data
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting data for event definition: #{event_definition.id}"
    )

    # Fifth remove all records from Elasticsearch
    case Elasticsearch.delete_by_query(Config.event_index_alias(), %{
           field: "event_definition_id",
           value: event_definition.id
         }) do
      {:ok, _count} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Elasticsearch data deleted for event definition: #{event_definition.id}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "There was an error deleting elasticsearch data for event definition: #{
            event_definition.id
          }\nError: #{inspect(error)}"
        )
    end

    case EventsContext.get_core_ids_for_event_definition_id(event_definition.id) do
      [] ->
        nil

      core_ids ->
        Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
          field: "id",
          value: core_ids
        })
    end

    # Sixth delete all event_definition_data. Anything linked to the
    # event_definition_id
    page =
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id}
        },
        page_number: 1,
        page_size: @page_size,
        preload_details: false,
        include_deleted: true
      )

    if length(page.entries) > 0 do
      delete_event_definition_data(page, event_definition.id)
    end

    # Delete notifications
    {_notification_settings_count, notification_settings} =
      NotificationsContext.hard_delete_notification_settings(%{
        filter: %{
          event_definition_id: event_definition.id
        },
        select: NotificationSetting.__schema__(:fields)
      })

    # Finally check if we are hard_deleting the event_definition or
    # just updating the deleted_at column
    if hard_delete_event_definitions do
      {:ok, %EventDefinition{} = deleted_event_definition} =
        EventsContext.hard_delete_event_definition(event_definition)

      ConsumerStateManager.remove_consumer_state(event_definition.id)
      DeleteEventDefinitionDataCache.remove_status(event_definition.id)

      Redis.publish_async(
        "event_definitions_subscription",
        %{deleted: EventsContext.remove_event_definition_virtual_fields(deleted_event_definition)}
      )
    else
      case EventsContext.update_event_definition(event_definition, %{
             active: false,
             started_at: nil
           }) do
        {:ok, %EventDefinition{} = updated_event_definition} ->
          ConsumerStateManager.remove_consumer_state(event_definition.id)
          DeleteEventDefinitionDataCache.remove_status(event_definition.id)

          Redis.publish_async(
            "event_definitions_subscription",
            %{updated: updated_event_definition.id}
          )

          Enum.each(notification_settings, fn notification_setting ->
            Redis.publish_async(
              "notification_settings_subscription",
              %{
                deleted:
                  NotificationsContext.remove_notification_setting_virtual_fields(
                    notification_setting
                  )
              }
            )
          end)

          CogyntLogger.info(
            "#{__MODULE__}",
            "Finished deleting data for event definition: #{updated_event_definition.id}"
          )

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Something went wrong deleting the event definition: #{inspect(error)}"
          )
      end
    end
  end

  defp delete_event_definition_data(
         %{entries: events, page_number: page_number, total_pages: total_pages},
         event_definition_id
       ) do
    event_ids = Enum.map(events, fn e -> e.id end)

    # Delete notifications
    {_notification_count, notifications} =
      NotificationsContext.hard_delete_notifications(%{
        filter: %{event_ids: event_ids},
        select: [:id]
      })

    notification_ids = Enum.map(notifications, fn n -> n.id end)

    # Delete notification collection items
    {_notification_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        filter: %{
          item_ids: notification_ids,
          item_type: "notification"
        }
      })

    # Delete event details
    {_event_detail_count, _} =
      EventsContext.hard_delete_event_details(%{
        filter: %{event_ids: event_ids}
      })

    # Delete events
    {_event_count, _} =
      EventsContext.hard_delete_events(%{
        filter: %{event_ids: event_ids}
      })

    # Delete event collection items
    {_event_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        filter: %{
          item_ids: event_ids,
          item_type: "event"
        }
      })

    if page_number >= total_pages do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished Removing All EventDefinitionData For DevDelete Action"
      )
    else
      next_page =
        EventsContext.get_page_of_events(
          %{
            filter: %{event_definition_id: event_definition_id}
          },
          page_number: page_number + 1,
          page_size: @page_size,
          preload_details: false,
          include_deleted: true
        )

      delete_event_definition_data(next_page, event_definition_id)
    end
  end
end
