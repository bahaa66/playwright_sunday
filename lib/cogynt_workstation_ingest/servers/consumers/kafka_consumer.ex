defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  @moduledoc """
  KafkaEx Gensconsumer module. Pulls messages from Kakfa and commits the offsets.
  Queues the messages to the appropriate Broadway Pipelines
  """
  use KafkaEx.GenConsumer

  alias CogyntWorkstationIngest.Broadway.{Producer, DrilldownProducer}

  @impl true
  def init(topic, _partition, %{event_definition: event_definition}) do
    {:ok, %{topic: topic, event_definition: event_definition}}
  end

  @impl true
  def init(_topic, _partition, _args) do
    {:ok, %{}}
  end

  @impl true
  def handle_message_set(message_set, %{event_definition: event_definition} = state) do
    type = event_definition.event_type
    Producer.enqueue(message_set, event_definition, type)
    {:sync_commit, state}
  end

  @impl true
  def handle_message_set(message_set, state) do
    DrilldownProducer.enqueue(message_set)
    {:sync_commit, state}
  end
end
