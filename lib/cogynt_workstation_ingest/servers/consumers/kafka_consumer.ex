defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  @moduledoc """
  KafkaEx Gensconsumer module. Pulls messages from Kakfa and commits the offsets.
  Queues the messages to the appropriate Broadway Pipelines
  """
  use KafkaEx.GenConsumer
  alias CogyntWorkstationIngest.Broadway.Producer
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
  def handle_message_set(message_set, %{event_definition: event_definition} = state) do
    Producer.enqueue(message_set, event_definition.id)
    # Small buffer from keeping the Broadway producer from getting overwhelmed with
    # kafka messagees
    # Process.sleep(500)
    {:sync_commit, state}
  end

  @impl true
  def handle_message_set(message_set, %{deployment: true} = state) do
    DeploymentsContext.handle_deployment_messages(message_set)
    {:sync_commit, state}
  end
end
