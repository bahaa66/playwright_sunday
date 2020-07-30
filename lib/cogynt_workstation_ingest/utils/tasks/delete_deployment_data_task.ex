defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDeploymentDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_deployment_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.Caches.DeploymentConsumerRetryCache

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(delete_topics) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_deployment_data_task with option delete_topics: #{delete_topics}"
    )

    delete_deployment_data(delete_topics)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_deployment_data(delete_topics) do
    CogyntLogger.info("#{__MODULE__}", "Stoping the Deployment ConsumerGroup")
    ConsumerGroupSupervisor.stop_child(:deployment)

    if delete_topics do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting the Deployment Topic: deployment"
      )

      delete_topic_result =
        KafkaEx.delete_topics(["deployment"],
          worker_name: :deployment_stream
        )

      CogyntLogger.info(
        "#{__MODULE__}",
        "Delete Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    end

    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data()
  end

  defp reset_deployment_data() do
    DeploymentsContext.hard_delete_deployments()
    Process.sleep(2000)
    CogyntLogger.info("#{__MODULE__}", "Starting the Deployment ConsumerGroup")

    case ConsumerGroupSupervisor.start_child(:deployment) do
      {:error, nil} ->
        CogyntLogger.warn("#{__MODULE__}", "Deployment Topic DNE. Adding to RetryCache")
        DeploymentConsumerRetryCache.retry_consumer(:deployment)

      _ ->
        CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
    end
  end
end
