defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteTopicDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
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
            :topic,
            :deployment_id
          ]
        },
        set: [active: false]
      )

    Enum.each(event_definition_data, fn %EventDefinition{
                                          id: id,
                                          topic: topic,
                                          deployment_id: deployment_id
                                        } ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Stoping ConsumerGroup for #{topic}"
      )

      ConsumerStateManager.manage_request(%{stop_consumer: id})

      if delete_topics do
        CogyntLogger.info("#{__MODULE__}", "Deleting Kakfa topic: #{topic}")
        worker_name = String.to_atom("deployment#{deployment_id}")
        KafkaEx.delete_topics([topic], worker_name: worker_name)
      end
    end)
  end
end
