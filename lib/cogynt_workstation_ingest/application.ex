defmodule CogyntWorkstationIngest.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  alias CogyntWorkstationIngest.Supervisors.{EventSupervisor, ConsumerGroupSupervisor}

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      # Start the Ecto repository
      CogyntWorkstationIngest.Repo,
      # Start the endpoint when the application starts
      CogyntWorkstationIngestWeb.Endpoint,
      # Start the DynamicSupervisor for the EventBroadway Pipeline
      EventSupervisor,
      # Start the DynamicSupervisor for KafkaEx ConsumerGroups
      ConsumerGroupSupervisor
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
end
