defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDeploymentDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Broadway.DeploymentPipeline
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Config

  def perform(delete_topics_for_deployments) do
    # First reset all DrilldownData passing true to delete topic data

    DeleteDrilldownDataWorker.perform(delete_topics_for_deployments)

    CogyntLogger.info("#{__MODULE__}", "Stopping the DeploymentPipeline")
    # Second shutdown the DeploymentPipeline
    Redis.publish_async("ingest_channel", %{shutdown_deployment_pipeline: "deployment"})

    ensure_deployment_pipeline_stopped()

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting the Deployment Topic: #{Config.deployment_topic()}"
    )

    # Third delete all data for the delployment topic
    if delete_topics_for_deployments do
      delete_topic_result = Kafka.Api.Topic.delete_topic(Config.deployment_topic())

      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    end

    # Fourth reset all the data for each event_definition
    DeleteEventDefinitionsAndTopicsWorker.perform(%{
      "event_definition_hash_ids" => [],
      "hard_delete" => true,
      "delete_topics" => delete_topics_for_deployments
    })

    # Finally reset all the deployment data
    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data()
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp ensure_deployment_pipeline_stopped(count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_deployment_pipeline_stopped/1 exceeded number of attempts. Moving forward with DeleteDeploymentData"
      )
    else
      case DeploymentPipeline.pipeline_running?() or
             not DeploymentPipeline.pipeline_finished_processing?() do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DeploymentPipeline still running... waiting for it to shutdown before resetting data"
          )

          Process.sleep(1000)
          ensure_deployment_pipeline_stopped(count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DeploymentPipeline stopped"
          )
      end
    end
  end

  defp reset_deployment_data() do
    DeploymentsContext.hard_delete_deployments()
    # Reset all JobQ Info
    ExqHelpers.flush_all()

    Redis.key_delete("dpcgid")
    Redis.key_delete("dpmi")
    Redis.key_delete("fdpm")
    Redis.hash_delete("crw", Config.deployment_topic())

    CogyntLogger.info("#{__MODULE__}", "Starting the DeploymentPipeline")

    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
