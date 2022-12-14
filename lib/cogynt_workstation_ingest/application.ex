defmodule CogyntWorkstationIngest.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Protocol
  # alias CogyntWorkstationIngest.Horde.{HordeRegistry, NodeObserver}
  # alias CogyntWorkstationIngest.Config
  # alias CogyntWorkstationIngest.Elasticsearch.IndexerSupervisor

  alias CogyntWorkstationIngest.Supervisors.{
    ConsumerGroupSupervisor,
    ServerSupervisor,
    TaskSupervisor,
    TelemetrySupervisor
  }

  alias CogyntWorkstationIngest.Tasks.RunAuditKafkaSetup
  alias Kafka.Supervisors.KafkaSetupTaskSupervisor

  def start(_type, _args) do
    kafka_setup_taks = [RunAuditKafkaSetup]

    # List all child processes to be supervised
    children = [
      {Phoenix.PubSub, [name: CogyntWorkstationIngestWeb.PubSub, adapter: Phoenix.PubSub.PG2]},
      # Start Horde an libcluster related supervisors. The registry needs to come before the TaskSupervisor.
      # HordeRegistry,
      # IndexerSupervisor,
      # {Cluster.Supervisor,
      #  [Config.libcluster_topologies(), [name: CogyntWorkstationIngest.ClusterSupervisor]]},
      # NodeObserver,
      # Start the Ecto repository
      CogyntWorkstationIngest.Repo,
      # Start the TelemetrySupervisor,
      TelemetrySupervisor,
      # Start the endpoint when the application starts
      CogyntWorkstationIngestWeb.Endpoint,
      # Start the Supervisor for Redis,
      child_spec_supervisor(RedisSupervisor, RedisSupervisor),
      # Start the Exq job queue Supervisor
      child_spec_supervisor(Exq, Exq),
      # Start the Cluster for Elasticsearch library
      CogyntWorkstationIngest.Elasticsearch.Cluster,
      # Start the Supervisor for all Genserver modules
      child_spec_supervisor(ServerSupervisor, ServerSupervisor),
      # Start the DynamicSupervisor for Kafka ConsumerGroups
      ConsumerGroupSupervisor,
      # The supervisor for all Task workers
      child_spec_supervisor(TaskSupervisor, TaskSupervisor),
      # Kafka setup task supervisor
      child_spec_supervisor(KafkaSetupTaskSupervisor, KafkaSetupTaskSupervisor, [
        kafka_setup_taks
      ])
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: CogyntWorkstationIngest.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  def config_change(changed, _new, removed) do
    CogyntWorkstationIngestWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp child_spec_supervisor(module_name, id, args \\ []) do
    %{
      id: id,
      start: {
        module_name,
        :start_link,
        args
      },
      restart: :permanent,
      shutdown: 5000,
      type: :supervisor
    }
  end
end
