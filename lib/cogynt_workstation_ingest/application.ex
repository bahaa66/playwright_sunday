defmodule CogyntWorkstationIngest.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  alias CogyntWorkstationIngest.Config

  alias CogyntWorkstationIngest.Supervisors.{
    ConsumerGroupSupervisor,
    ServerSupervisor,
    TaskSupervisor
  }

  alias Redis.Supervisors.{
    RedisSingleInstanceSupervisor,
    RedisConnectionPoolSupervisor
  }

  alias CogyntWorkstationIngest.Servers.Startup
  alias CogyntWorkstationIngest.Broadway.{EventPipeline, LinkEventPipeline, DrilldownPipeline}

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      # Start the Ecto repository
      CogyntWorkstationIngest.Repo,
      # Start the endpoint when the application starts
      CogyntWorkstationIngestWeb.Endpoint,
      # Start the Supervisor for Redis,
      child_spec_supervisor(RedisSingleInstanceSupervisor, RedisSingleInstanceSupervisor),
      # Start the Supervisor for the Broadway EventPipeline
      EventPipeline,
      # Start the Supervisor for the Broadway LinkEventPipeline
      LinkEventPipeline,
      # Start the Supervisor for the Broadway DrilldownPipeline
      DrilldownPipeline,
      # Start the DynamicSupervisor for KafkaEx ConsumerGroups
      ConsumerGroupSupervisor,
      # The supervisor for all Task workers
      TaskSupervisor,
      # Start the Supervisor for all Genserver modules
      child_spec_supervisor(ServerSupervisor, ServerSupervisor)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: CogyntWorkstationIngest.Supervisor]
    result = Supervisor.start_link(children, opts)

    Process.send_after(Startup, :initialize_consumers, Config.startup_delay())

    result
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  def config_change(changed, _new, removed) do
    CogyntWorkstationIngestWeb.Endpoint.config_change(changed, removed)
    :ok
  end

  defp child_spec_supervisor(module_name, id) do
    %{
      id: id,
      start: {
        module_name,
        :start_link,
        []
      },
      restart: :permanent,
      shutdown: 5000,
      type: :supervisor
    }
  end
end
