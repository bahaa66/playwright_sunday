defmodule CogyntWorkstationIngest.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application
  require Protocol

  Protocol.derive(Jason.Encoder, Broadway.Message,
    only: [
      :batch_key,
      :batch_mode,
      :batcher,
      :data,
      :metadata
    ]
  )

  alias CogyntWorkstationIngest.Supervisors.{
    ConsumerGroupSupervisor,
    ServerSupervisor,
    TaskSupervisor,
    TelemetrySupervisor
  }

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
      # Start the Exq job queue Supervisor
      child_spec_supervisor(Exq, Exq),
      # Start the Supervisor for all Genserver modules
      child_spec_supervisor(ServerSupervisor, ServerSupervisor),
      # Start the DynamicSupervisor for Kafka ConsumerGroups
      ConsumerGroupSupervisor,
      # The supervisor for all Task workers
      child_spec_supervisor(TaskSupervisor, TaskSupervisor)
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
