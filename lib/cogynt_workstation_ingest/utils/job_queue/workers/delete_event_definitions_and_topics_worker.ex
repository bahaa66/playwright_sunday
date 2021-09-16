defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteEventDefinitionsAndTopicsWorker do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

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
        # have to fetch the consumer_group_name before we shutdown the pipeline. That action
        # removes the consumer_group_name from Redis and we wont be able to fetch it after it
        # completes
        datasource_name = ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id)
        # 1) stop the EventPipeline if there is one running for the event_definition
        shutdown_event_pipeline(event_definition)

        # 2) check to see if the topic needs to be deleted
        if delete_topics do
          delete_topics(event_definition)
        end

        # 3) remove all records from Elasticsearch
        delete_elasticsearch_data(event_definition)

        # 4) remove Druid datasource
        delete_druid_datasource(datasource_name)

        ConsumerStateManager.remove_consumer_state(event_definition.id)
      end)

      EventsContext.truncate_all_tables()

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
              "EventDefinition not found for event_definition_id: #{event_definition_id}"
            )

          event_definition ->
            # have to fetch the consumer_group_name before we shutdown the pipeline. That action
            # removes the consumer_group_name from Redis and we wont be able to fetch it after it
            # completes
            datasource_name = ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id)
            # 1) stop the EventPipeline if there is one running for the event_definition
            shutdown_event_pipeline(event_definition)

            # 2) check to see if the topic needs to be deleted
            if delete_topics do
              delete_topics(event_definition)
            end

            # 3) remove all records from Elasticsearch
            delete_elasticsearch_data(event_definition)

            # 4) delete the event definition data
            delete_event_definition(event_definition)

            # 5) one last call to remove elasticsearch data to ensure all data has been removed from all shards
            delete_elasticsearch_data(event_definition)

            # 6) remove Druid datasource
            delete_druid_datasource(datasource_name)
        end
      end)
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp shutdown_event_pipeline(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Shutting down EventPipeline for #{event_definition.topic}"
    )

    {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

    if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
      Redis.publish_async("ingest_channel", %{
        shutdown_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    end

    ensure_pipeline_shutdown(event_definition.id)
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
    EventsContext.hard_delete_by_event_definition_id(event_definition.id)

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

    # TBD delete 
    # case Elasticsearch.delete_by_query(Config.event_index_alias(), %{
    #        field: "event_definition_id",
    #        value: event_definition.id
    #      }) do
    #   {:ok, _count} ->
    #     CogyntLogger.info(
    #       "#{__MODULE__}",
    #       "Elasticsearch data deleted for EventDefinitionId: #{event_definition.id}"
    #     )

    #   {:error, error} ->
    #     CogyntLogger.error(
    #       "#{__MODULE__}",
    #       "There was an error deleting elasticsearch data for event definition: #{
    #         event_definition.id
    #       }\nError: #{inspect(error)}"
    #     )
    # end
  end

  defp delete_druid_datasource(name) do
    IO.puts("********* Deleting Druid DataSource for Name: #{name}")

    case Druid.delete_datasource(name) do
      {:ok, result} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Deleted Druid Datasource with response: #{inspect(result)}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to remove Druid datasource with Error: #{inspect(error)}"
        )
    end
  end

  defp ensure_pipeline_shutdown(event_definition_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_pipeline_shutdown/1 exceeded number of attempts. Moving forward with DeleteEventDefinitionsAndTopics"
      )
    else
      {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition_id)

      case EventPipeline.pipeline_started?(event_definition_id) or
             not EventPipeline.pipeline_finished_processing?(event_definition_id) or
             consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before resetting data"
          )

          Process.sleep(1000)
          ensure_pipeline_shutdown(event_definition_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} shutdown"
          )
      end
    end
  end
end
