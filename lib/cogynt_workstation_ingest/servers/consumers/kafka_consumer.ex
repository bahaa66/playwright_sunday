defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  @moduledoc """
  KafkaEx Gensconsumer module. Pulls messages from Kakfa and commits the offsets.
  Queues the messages to the appropriate Broadway Pipelines
  """
  use KafkaEx.GenConsumer
  alias CogyntWorkstationIngest.Broadway.{Producer, DrilldownProducer}
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  # TODO eventually do a POC on using another external Kafka consumer service

  @impl true
  def init(topic, _partition, %{event_definition: event_definition}) do
    Redis.hash_increment_by("b:#{event_definition.id}", "tmc", 0)
    Redis.hash_increment_by("b:#{event_definition.id}", "tmp", 0)

    {:ok, %{topic: topic, event_definition: event_definition}}
  end

  @impl true
  def init("deployment", _partition, _args) do
    {:ok, %{deployment: true}}
  end

  @impl true
  def init(_topic, _partition, _args) do
    Redis.hash_increment_by("drilldown_message_info", "tmc", 0)
    Redis.hash_increment_by("drilldown_message_info", "tmp", 0)

    {:ok, %{drilldown: true}}
  end

  @impl true
  def handle_message_set(message_set, %{event_definition: event_definition} = state) do
    type = event_definition.event_type
    Producer.enqueue(message_set, event_definition.id, type)
    {:sync_commit, state}
  end

  @impl true
  def handle_message_set(message_set, %{deployment: true} = state) do
    DeploymentsContext.handle_deployment_messages(message_set)
    {:sync_commit, state}
  end

  @impl true
  def handle_message_set(message_set, %{drilldown: true} = state) do
    IO.inspect(Enum.count(message_set), label: "*** Messages Pulled From Drilldown Topic")
    DrilldownProducer.enqueue(message_set)
    {:sync_commit, state}
  end
end
