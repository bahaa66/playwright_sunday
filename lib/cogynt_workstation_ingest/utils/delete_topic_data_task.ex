defmodule CogyntWorkstationIngest.Utils.DeleteTopicDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Servers.ConsumerStateManager
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngestWeb.Rpc.{IngestHandler, CogyntClient}

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

        ConsumerStateManager.manage_request(%{stop_consumer: topic})

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
