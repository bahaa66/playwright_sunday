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
    DynamicTaskSupervisor,
    TaskSupervisor,
    TelemetrySupervisor
  }

  alias CogyntWorkstationIngest.Config

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
      exq_job_queue_child_spec(),
      # Start the Supervisor for all Genserver modules
      child_spec_supervisor(ServerSupervisor, ServerSupervisor),
      # Start the DynamicSupervisor for Kafka ConsumerGroups
      ConsumerGroupSupervisor,
      # The dynamic supervisor for all Task workers
      DynamicTaskSupervisor,
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

  # TODO: REMOVE THIS METHOD ONCE WE ARE USING SEPERATE CONFIGS FOR DEV AND PROD
  # only have to do this right now because on local envs we are not using Redis Sentinels
  # but on develop and prod envs we are. And there is no way to dynamically set the redis options to use
  # Sentinel configs just using the dev.exs config file. Need to be using the dev.exs and prod.exs config files
  defp exq_job_queue_child_spec() do
    exq_configs = Application.get_all_env(:exq)

    IO.inspect(exq_configs, label: "EXQ CONFIGS")

    case Config.redis_instance() do
      :sentinel ->
        IO.inspect(Config.redis_instance(), label: "REDIS INSTANCE")
        sentinels = String.split(Config.redis_sentinels(), ",", trim: true)

        exq_configs =
          exq_configs ++
            [
              redis_options: [
                sentinel: [sentinels: sentinels, group: Config.redis_sentinel_group()],
                password: Config.redis_password()
              ]
            ]

        child_spec_supervisor(
          Exq,
          Exq,
          [exq_configs]
        )

      _ ->
        exq_configs =
          exq_configs ++
            [
              redis_options: [
                host: Config.redis_host(),
                password: Config.redis_password()
              ]
            ]

        child_spec_supervisor(
          Exq,
          Exq,
          [exq_configs]
        )
    end
  end

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
