defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteEventDefinitionsAndTopicsWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Collections.CollectionsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  def perform(%{
        "event_definition_ids" => event_definition_ids,
        "hard_delete" => hard_delete_event_definitions,
        "delete_topics" => delete_topics
      }) do
    if hard_delete_event_definitions do
      EventsContext.list_event_definitions()
      |> Enum.each(fn event_definition ->
        # First stop the EventPipeline if there is one running for the event_definition
        stop_event_pipeline(event_definition)

        # Second check to see if the topic needs to be deleted
        if delete_topics do
          delete_topics(event_definition)
        end

        # Third remove all records from Elasticsearch
        delete_elasticsearch_data(event_definition)

        ConsumerStateManager.remove_consumer_state(event_definition.id,
          hard_delete_event_definition: true
        )
      end)

      truncate_all_tables()

      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished deleting data for EventDefinitionIds: #{inspect(event_definition_ids)}"
      )
    else
      Enum.each(event_definition_ids, fn event_definition_id ->
        case EventsContext.get_event_definition(event_definition_id) do
          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Event definition not found for event_definition_id: #{event_definition_id}"
            )

          event_definition ->
            # First stop the EventPipeline if there is one running for the event_definition
            stop_event_pipeline(event_definition)

            # Second check to see if the topic needs to be deleted
            if delete_topics do
              delete_topics(event_definition)
            end

            # Third remove all records from Elasticsearch
            delete_elasticsearch_data(event_definition)

            # Fourth delete the event definition data
            delete_event_definition(event_definition)

            # Finally one last call to remove elasticsearch data to ensure all data has been removed from all shards
            delete_elasticsearch_data(event_definition)
        end
      end)
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

  defp delete_topics(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting Kakfa topic: #{event_definition.topic}"
    )

    case DeploymentsContext.get_kafka_brokers(event_definition.deployment_id) do
      {:ok, brokers} ->
        Kafka.Api.Topic.delete_topic(event_definition.topic, brokers)

      {:error, :does_not_exist} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to fetch brokers for DeploymentId: #{event_definition.deployment_id}"
        )
    end
  end

  defp delete_event_definition(event_definition) do
    # delete data
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting data for EventDefinitionId: #{event_definition.id}"
    )

    # Sixth delete all event_definition_data. Anything linked to the
    # event_definition_id
    EventsContext.query_events(%{
      filter: %{event_definition_id: event_definition.id},
      select: [:id],
      order_by: :created_at,
      limit: 2000
    })
    |> delete_event_definition_data(event_definition.id)

    # Finally update the EventDefintion to be inactive
    case EventsContext.update_event_definition(event_definition, %{
           active: false
         }) do
      {:ok, %EventDefinition{} = updated_event_definition} ->
        ConsumerStateManager.remove_consumer_state(event_definition.id)

        Redis.publish_async(
          "event_definitions_subscription",
          %{updated: updated_event_definition.id}
        )

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

  defp delete_elasticsearch_data(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting Elasticsearch data for EventDefinitionId: #{event_definition.id}"
    )

    case Elasticsearch.delete_by_query(Config.event_index_alias(), %{
           field: "event_definition_id",
           value: event_definition.id
         }) do
      {:ok, _count} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Elasticsearch data deleted for EventDefinitionId: #{event_definition.id}"
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
  end

  defp delete_event_definition_data(events, event_definition_id) do
    if Enum.empty?(events) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished Removing All EventDefinitionData For DevDelete Action"
      )
    else
      CogyntLogger.info(
        "#{__MODULE__}",
        "Removing Events and Associated Data W/ EventCount: #{Enum.count(events)}"
      )

      event_ids = Enum.map(events, fn e -> e.id end)

      # Delete notifications
      {_notification_count, notifications} =
        NotificationsContext.hard_delete_notifications(%{
          filter: %{event_ids: event_ids},
          select: [:id]
        })

      notification_ids = Enum.map(notifications, fn n -> n.id end)

      # Delete notification_settings
      {_notification_settings_count, _notification_settings} =
        NotificationsContext.hard_delete_notification_settings(%{
          filter: %{
            event_definition_id: event_definition_id
          }
        })

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

      # Delete event links
      {_event_link_count, _} =
        EventsContext.hard_delete_event_links(%{
          filter: %{linkage_event_ids: event_ids}
        })

      # Delete event collection items
      {_event_item_count, _} =
        CollectionsContext.hard_delete_collection_items(%{
          filter: %{
            item_ids: event_ids,
            item_type: "event"
          }
        })

      # Delete events
      {_event_count, _} =
        EventsContext.hard_delete_events(%{
          filter: %{event_ids: event_ids}
        })

      # Delete event detail templates
      {_event_detail_template_count, _} =
        EventsContext.hard_delete_event_detail_templates(%{
          filter: %{event_definition_id: event_definition_id}
        })

      # Delete event detail templates
      {_event_definition_details_count, _} =
        EventsContext.hard_delete_event_definition_details(event_definition_id)

      new_events =
        EventsContext.query_events(%{
          filter: %{event_definition_id: event_definition_id},
          select: [:id],
          order_by: :created_at,
          limit: 2000
        })

      delete_event_definition_data(new_events, event_definition_id)
    end
  end

  defp ensure_event_pipeline_stopped(event_definition_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_event_pipeline_stopped/1 exceeded number of attempts. Moving forward with DeleteEventDefinitionsAndTopics"
      )
    else
      case EventPipeline.event_pipeline_running?(event_definition_id) or
             not EventPipeline.event_pipeline_finished_processing?(event_definition_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before resetting data"
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

  def truncate_all_tables() do
    try do
      {:ok, result = %Postgrex.Result{}} =
        Repo.query(
          "SELECT truncate_tables('#{Config.postgres_username()}')",
          []
        )

      CogyntLogger.info(
        "#{__MODULE__}",
        "truncate_all_tables completed with result: #{result.connection_id}"
      )
    rescue
      _ ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "truncate_all_tables failed"
        )
    end
  end
end
