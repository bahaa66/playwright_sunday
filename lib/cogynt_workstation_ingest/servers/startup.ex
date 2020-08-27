defmodule CogyntWorkstationIngest.Servers.Startup do
  @moduledoc """
  Genserver Module that is used for tasks that need to run upon Application startup
  """
  use GenServer
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.Caches.DeploymentConsumerRetryCache

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_info(:initialize_consumers, state) do
    CogyntLogger.info("#{__MODULE__}", "Initializing Consumers")
    initialize_consumers()
    {:noreply, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #l
  defp initialize_consumers() do
    with :ok <- Application.ensure_started(:phoenix),
         :ok <- Application.ensure_started(:postgrex) do
      EventsContext.initalize_consumer_states()
      CogyntLogger.info("#{__MODULE__}", "Consumers Initialized")

      case ConsumerGroupSupervisor.start_child(:deployment) do
        {:error, nil} ->
          CogyntLogger.warn("#{__MODULE__}", "Deployment Topic DNE. Adding to RetryCache")
          DeploymentConsumerRetryCache.retry_consumer(:deployment)

        _ ->
          CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
      end
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "CogyntWorkstationIngest Application not started. #{inspect(error, pretty: true)}"
        )
    end

    case DeploymentsContext.list_deployments() do
      nil ->
        nil

      deployments ->
        Enum.each(deployments, fn deployment ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "Starting DrilldownConsumer for Deplpoyment_ID: #{deployment.id}"
          )

          ConsumerGroupSupervisor.start_child(:drilldown, deployment)
        end)
    end
  end
end
