defmodule CogyntWorkstationIngest.KafkaConsumer do
  use KafkaEx.GenConsumer
  alias CogyntWorkstationIngest.Supervisors.EventSupervisor
  alias CogyntWorkstationIngest.Broadway.EventProducer

  @impl true
  def init(topic, _partition, args) do
    event_definition = args[:event_definition]
    EventSupervisor.start_child(event_definition)
    {:ok, %{topic: topic, event_definition: event_definition}}
  end

  @impl true
  def handle_message_set(message_set, %{event_definition: event_definition} = state) do
    EventProducer.enqueue(message_set, event_definition.topic)
    {:sync_commit, state}
  end
end
