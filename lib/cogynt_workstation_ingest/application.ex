defmodule CogyntWorkstationIngest.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  alias CogyntWorkstationIngest.Config

  alias CogyntWorkstationIngest.Supervisors.{
    ConsumerGroupSupervisor,
    ServerSupervisor,
    TaskSupervisor,
    TelemetrySupervisor
  }

  alias CogyntWorkstationIngest.Servers.Startup

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      {Phoenix.PubSub, [name: CogyntWorkstationIngestWeb.PubSub, adapter: Phoenix.PubSub.PG2]},
      # Start the Ecto repository
      CogyntWorkstationIngest.Repo,
      # Start the TelemetrySupervisor,
      TelemetrySupervisor,
      # Start the endpoint when the application starts
      CogyntWorkstationIngestWeb.Endpoint,
      # Start the Supervisor for Redis,
      child_spec_supervisor(RedisSupervisor, RedisSupervisor),
      # Start the Supervisor for all Genserver modules
      child_spec_supervisor(ServerSupervisor, ServerSupervisor),
      # Start the DynamicSupervisor for KafkaEx ConsumerGroups
      ConsumerGroupSupervisor,
      # The supervisor for all Task workers
      TaskSupervisor
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
