defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  use KafkaEx.GenConsumer

  alias CogyntWorkstationIngest.Supervisors.{
    EventSupervisor,
    LinkEventSupervisor,
    DrilldownSupervisor
  }

  alias CogyntWorkstationIngest.Broadway.{EventProducer, LinkEventProducer, DrilldownProducer}
  alias CogyntWorkstationIngestWeb.Rpc.IngestClient

  @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]

  @impl true
  def init(topic, _partition, %{event_definition: event_definition}) do
    if link_event?(event_definition) do
      LinkEventSupervisor.start_child(event_definition)
    else
      EventSupervisor.start_child(event_definition)
    end

    {:ok, %{topic: topic, event_definition: event_definition}}
  end

  @impl true
  def init(_topic, _partition, _args) do
    DrilldownSupervisor.start_child()
    {:ok, %{}}
  end

  @impl true
  def handle_message_set(message_set, %{event_definition: event_definition} = state) do
    if link_event?(event_definition) do
      LinkEventProducer.enqueue(message_set, event_definition.topic)
    else
      EventProducer.enqueue(message_set, event_definition.topic)
    end

    IngestClient.publish_event_definition_ids([event_definition.id])
    {:sync_commit, state}
  end

  @impl true
  def handle_message_set(message_set, state) do
    DrilldownProducer.enqueue(message_set)
    {:sync_commit, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp link_event?(%{event_type: type}), do: type == @linkage
end
