defmodule CogyntWorkstationIngest.Supervisors.ServerSupervisor do
  @moduledoc """
  Supervisor for all CogyntWorkstationIngest GenServer modules
  """
  use Supervisor

  alias CogyntWorkstationIngest.Servers.Workers.{
    ConsumerRetryWorker,
    RedisStreamsConsumerGroupWorker
  }

  alias CogyntWorkstationIngest.Servers.PubSub.{
    IngestPubSub
  }

  alias CogyntWorkstationIngest.Servers.{ConsumerMonitor, BroadwayProducerMonitor}

  alias CogyntElasticsearch.Indexer
  alias CogyntWorkstationIngest.Config
  # alias CogyntWorkstationIngest.Elasticsearch.IndexerStarter

  @singleton_pod "ws-ingest-otp-0"

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Start Redis pub/sub
    {:ok, pubsub} = Redis.pub_sub_start()

    children = [
      child_spec(ConsumerRetryWorker),
      # child_spec(FailedMessagesRetryWorker),
      child_spec(RedisStreamsConsumerGroupWorker, restart: :permanent),
      child_spec(ConsumerMonitor, restart: :permanent),
      child_spec(BroadwayProducerMonitor, restart: :permanent),
      child_spec(IngestPubSub, restart: :permanent, start_link_opts: [pubsub])
      # {IndexerStarter, [name: Indexer]}
    ]

    # This is a hacky solution to create any service as a singleton only running on
    # pod-name-0. This is because we currently cannot get Libcluster working with Istio
    children =
      if Config.pod_name() == @singleton_pod do
        children ++ [child_spec(Indexer, restart: :temporary)]
      else
        children
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp child_spec(module_name, opts \\ []) do
    restart_type = Keyword.get(opts, :restart, :transient)
    start_link_opts = Keyword.get(opts, :start_link_opts, [])

    %{
      id: module_name,
      start: {
        module_name,
        :start_link,
        start_link_opts
      },
      restart: restart_type,
      shutdown: 5000,
      type: :worker
    }
  end
end
