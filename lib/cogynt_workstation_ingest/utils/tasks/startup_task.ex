defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task

  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.Caches.DeploymentConsumerRetryCache

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
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
