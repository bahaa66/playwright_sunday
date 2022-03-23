defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteEventDefinitionsAndTopicsWorker do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi

  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  def perform(
        %{
          "event_definition_hash_id" => event_definition_hash_id,
          "delete_topics" => delete_topics
        } = args
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "RUNNING DELETE EVENTDEFINITION DATA WORKER. Args: #{inspect(args, pretty: true)}"
    )

    case EventsContext.get_event_definition(event_definition_hash_id) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "EventDefinition not found for event_definition_hash_id: #{event_definition_hash_id}"
        )

      event_definition ->
        # {_, state} = ConsumerStateManager.get_consumer_state(event_definition.id)

        # if !is_nil(state.topic) do
        # 1) stop the EventPipeline if there is one running for the event_definition
        shutdown_event_pipeline(event_definition)

        # 2) check to see if the topic needs to be deleted
        if delete_topics do
          delete_topics(event_definition)
        end

        # 4) drop druid data and terminate supervisor
        drop_and_terminate_druid(event_definition.topic)

        # 4) remove all records from Elasticsearch
        delete_elasticsearch_data(event_definition)

        # 5) delete the event definition data
        delete_event_definition(event_definition)
        # end
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp shutdown_event_pipeline(event_definition) do
    {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

    if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
      Redis.publish_async("ingest_channel", %{
        shutdown_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    end

    ensure_pipeline_shutdown(event_definition.id)
  end

  defp delete_topics(event_definition) do
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
    # Sixth delete all event_definition_data. Anything linked to the event_definition_hash_id
    EventsContext.hard_delete_by_event_definition_hash_id(event_definition.id)

    # Finally update the EventDefintion to be inactive
    case EventsContext.update_event_definition(event_definition, %{
           active: false
         }) do
      {:ok, %EventDefinition{} = updated_event_definition} ->
        ConsumerStateManager.remove_consumer_state(updated_event_definition.id)

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Something went wrong deleting the event definition: #{inspect(error)}"
        )
    end
  end

  defp delete_elasticsearch_data(event_definition) do
    case ElasticApi.delete_by_query(Config.event_index_alias(), %{
           field: "event_definition_hash_id",
           value: event_definition.id
         }) do
      {:ok, _count} ->
        {:ok, :success}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "There was an error deleting elasticsearch data for event definition: #{event_definition.id}\nError: #{inspect(error)}"
        )
    end
  end

  defp drop_and_terminate_druid(datasource_name) do
    case DruidRegistryHelper.drop_and_terminate_druid_with_registry_lookup(datasource_name) do
      {:ok, result} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Dropped segments for Druid Datasource: #{datasource_name} with response: #{inspect(result)}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to drop segments for Druid Datasource: #{datasource_name} with Error: #{inspect(error)}"
        )
    end
  end

  defp ensure_pipeline_shutdown(event_definition_hash_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_pipeline_shutdown/1 exceeded number of attempts (30) Moving forward with DeleteEventDefinitionsAndTopics"
      )
    else
      {_status, consumer_state} =
        ConsumerStateManager.get_consumer_state(event_definition_hash_id)

      case EventPipeline.pipeline_started?(event_definition_hash_id) or
             not EventPipeline.pipeline_finished_processing?(event_definition_hash_id) or
             consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_hash_id} still running... waiting 1000 ms for it to shutdown before resetting data"
          )

          Process.sleep(1000)
          ensure_pipeline_shutdown(event_definition_hash_id, count + 1)

        false ->
          nil
      end
    end
  end
end
