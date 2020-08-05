defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteTopicDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Servers.Caches.DeleteEventDefinitionDataCache

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(
        %{
          event_definition_ids: event_definition_ids,
          hard_delete_event_definitions: hard_delete_event_definitions,
          delete_topics: delete_topics
        } = args
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_topic_data_task for event_definition_ids: #{event_definition_ids}, hard_delete_event_definitions: #{
        hard_delete_event_definitions
      }, delete_topics: #{delete_topics}"
    )

    Enum.each(event_definition_ids, fn event_definition_id ->
      delete_event_definition(event_definition_id, hard_delete_event_definitions, delete_topics)
    end)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_event_definition(event_definition_id, hard_delete_event_definitions, delete_topics) do
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
          KafkaEx.delete_topics([event_definition.topic], worker_name: worker_name)
        end

        # Third check the consumer_state to make sure if it has any data left in the pipeline
        {:ok, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

        cond do
          consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
            # If data is still processing add event_definition_id to the cache to
            # trigger the delete task when it is done
            DeleteEventDefinitionDataCache.update_status(event_definition.id,
              status: :waiting,
              hard_delete: hard_delete_event_definitions
            )

          true ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleting data for event definition: #{event_definition_id}"
            )

            if hard_delete_event_definitions do
              # Remove records from elasticsearch first
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

              EventsContext.hard_delete_event_definition(event_definition)
              ConsumerStateManager.remove_consumer_state(event_definition.id)
            else
              # Fetch all events to delete
              %{entries: events, metadata: metadata} =
                EventsContext.get_page_of_events(
                  %{event_definition_id: event_definition.id},
                  %{limit: Config.delete_data_event_page_limit()}
                )

              counts =
                if length(events) > 0 do
                  delete_event_definition_data(events, metadata, event_definition.id)
                else
                  %{}
                end

              # Delete notifications
              {notification_settings_count, notification_settings} =
                NotificationsContext.hard_delete_notification_settings(%{
                  event_definition_id: event_definition.id,
                  select: NotificationSetting.__schema__(:fields)
                })

              counts = Map.put(counts, :notification_settings, notification_settings_count)

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

              case EventsContext.update_event_definition(event_definition, %{
                     active: false,
                     started_at: nil
                   }) do
                {:ok, %EventDefinition{} = updated_event_definition} ->
                  case remove_event_defintion(event_definition.id) do
                    {:ok, _event_definition_id} ->
                      EventsContext.remove_event_definition_consumer_state(
                        updated_event_definition.id
                      )

                      # Trigger ingest pub/sub subscription
                      # Absinthe.Subscription.publish(
                      #   CogyntWeb.Endpoint,
                      #   Map.merge(
                      #     updated_event_definition,
                      #     %{
                      #       is_being_deleted: false,
                      #       active: false
                      #     }
                      #   ),
                      #   event_definition_deleted: "event_definition_deleted:*"
                      # )

                      if !hard_delete do
                        Enum.each(notification_settings, fn ns ->
                          nil
                          # Trigger ingest pub/sub subscription
                          # Absinthe.Subscription.publish(
                          #   CogyntWeb.Endpoint,
                          #   ns,
                          #   notification_setting_deleted: "notification_setting_deleted:*"
                          # )
                        end)
                      end

                      CogyntLogger.info(
                        "#{__MODULE__}",
                        "Finished deleting data for event definition: #{
                          updated_event_definition.id
                        }\ndeletion_counts: #{inspect(counts)}"
                      )

                      {:ok, updated_event_definition}
                  end

                {:error, error} ->
                  CogyntLogger.error(
                    "#{__MODULE__}",
                    "Something went wronng deleting the event definition: #{inspect(error)}"
                  )

                  {:error, error}
              end
            end
        end
    end
  end

  defp delete_event_definition_data(events, %{after: after_cursor}, event_definition_id) do
    event_ids = Enum.map(events, fn e -> e.id end)

    # Delete notifications
    {notification_count, notifications} =
      NotificationsContext.hard_delete_notifications(%{event_ids: event_ids, select: [:id]})

    notification_ids = Enum.map(notifications, fn n -> n.id end)

    # Delete notification collection items
    {notification_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        item_ids: notification_ids,
        item_type: "notification"
      })

    # Delete event details
    {event_detail_count, _} = EventsContext.hard_delete_event_details(%{event_ids: event_ids})

    # Delete events
    {event_count, _} = EventsContext.hard_delete_events(%{ids: event_ids})

    # Delete event collection items
    {event_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        item_ids: event_ids,
        item_type: "event"
      })

    if after_cursor != nil do
      # If there is an after cursor then we continue to the next page.
      %{entries: events, metadata: metadata} =
        EventsContext.get_page_of_events(%{event_definition_id: event_definition_id}, %{
          limit: Config.delete_data_event_page_limit(),
          after: after_cursor
        })

      %{
        event_collection_items: e_i_count,
        event_details: e_d_count,
        events: e_count,
        notification_collection_items: n_i_count,
        notifications: n_count
      } = delete_event_definition_data(events, metadata, event_definition_id)

      %{
        event_collection_items: e_i_count + event_item_count,
        event_details: e_d_count + event_detail_count,
        events: e_count + event_count,
        notification_collection_items: n_i_count + notification_item_count,
        notifications: n_count + notification_count
      }
    else
      %{
        event_collection_items: event_item_count,
        event_details: event_detail_count,
        events: event_count,
        notification_collection_items: notification_item_count,
        notifications: notification_count
      }
    end
  end
end
