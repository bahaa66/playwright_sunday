defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    EventsContext.initalize_consumer_states()

    # TODO: This needs to call a Redis Pub/Sub method in order to ensure this
    # Pipeline is started on multiple pods
    case ConsumerGroupSupervisor.start_child(:deployment) do
      {:error, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Deployment topic DNE. Will retry to create consumer..."
        )

        Redis.hash_set_async("crw", Config.deployment_topic(), "dp")

      _ ->
        CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
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

          # TODO: This needs to call a Redis Pub/Sub method in order to ensure this
          # Pipeline is started on multiple pods
          ConsumerGroupSupervisor.start_child(:drilldown, deployment)
        end)
    end
  end
end
