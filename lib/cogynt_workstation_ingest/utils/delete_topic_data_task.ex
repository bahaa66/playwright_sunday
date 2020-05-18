defmodule CogyntWorkstationIngest.Utils.DeleteTopicDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngestWeb.Rpc.{CogyntClient, IngestHandler}
  alias Models.Events.EventDefinition

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(%{event_definition_ids: event_definition_ids, delete_topics: delete_topics} = args) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_topic_data_task for event_definition_ids: #{event_definition_ids}, delete_topics: #{
        delete_topics
      }"
    )

    delete_topic_data(args)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_topic_data(%{
         event_definition_ids: event_definition_ids,
         delete_topics: delete_topics
       }) do
    {_count, event_definition_data} =
      EventsContext.update_event_definitions(
        %{
          filter: %{event_definition_ids: event_definition_ids},
          select: [
            :id,
            :topic
          ]
        },
        set: [active: false]
      )

    topics =
      Enum.reduce(event_definition_data, [], fn %EventDefinition{
                                                  id: id,
                                                  topic: topic
                                                },
                                                acc ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Stoping ConsumerGroup for #{topic}"
        )

        ConsumerGroupSupervisor.stop_child(topic)

        CogyntLogger.info(
          "#{__MODULE__}",
          "Deleting Elasticsearch data for #{id}"
        )

        {:ok, _} =
          Elasticsearch.delete_by_query(Config.event_index_alias(), %{
            field: "event_definition_id",
            value: id
          })

        case EventsContext.get_core_ids_for_event_definition_id(id) do
          nil ->
            nil

          core_ids ->
            Enum.each(core_ids, fn core_id ->
              {:ok, _} =
                Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
                  field: "id",
                  value: core_id
                })
            end)
        end

        %{status: :ok, body: result} =
          IngestHandler.handle_request("ingest:check_status", [%{"id" => id, "topic" => topic}])

        consumer_status = List.first(result)

        CogyntLogger.info(
          "#{__MODULE__}",
          "Publishing consumer status. Status: #{consumer_status.status}. For ID: #{id}"
        )

        CogyntClient.publish_consumer_status(id, topic, consumer_status.status)

        acc ++ [topic]
      end)

    if delete_topics do
      CogyntLogger.info("#{__MODULE__}", "Deleting Kakfa topics for #{topics}")

      KafkaEx.delete_topics(topics, worker_name: :standard)
    end
  end
end
