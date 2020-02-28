defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  @moduledoc """
  KafkaEx Gensconsumer module. Pulls messages from Kakfa and commits the offsets.
  Queues the messages to the appropriate Broadway Pipelines
  """
  use KafkaEx.GenConsumer

  alias CogyntWorkstationIngest.Supervisors.DrilldownSupervisor
  alias CogyntWorkstationIngest.Broadway.{Producer, DrilldownProducer}
  alias CogyntWorkstationIngestWeb.Rpc.IngestClient

  @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]

  @impl true
  def init(topic, _partition, %{event_definition: event_definition}) do
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
      Producer.enqueue(message_set, event_definition, :linkevent)
    else
      Producer.enqueue(message_set, event_definition, :event)
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
