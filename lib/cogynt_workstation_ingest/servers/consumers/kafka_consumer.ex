defmodule CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer do
  @moduledoc """
  KafkaEx Gensconsumer module. Pulls messages from Kakfa and commits the offsets.
  Queues the messages to the appropriate Broadway Pipelines
  """
  use KafkaEx.GenConsumer
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  # TODO eventually do a POC on using another external Kafka consumer service

  @impl true
  def init("deployment", _partition, _args) do
    {:ok, %{deployment: true}}
  end

  @impl true
  def handle_message_set(message_set, %{deployment: true} = state) do
    DeploymentsContext.handle_deployment_messages(message_set)
    {:sync_commit, state}
  end
end
