defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDeploymentDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Broadway.DeploymentPipeline
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntWorkstationIngest.Config

  def perform() do
    # First reset all DrilldownData passing true to delete topic data
    DeleteDrilldownDataWorker.perform(true)

    CogyntLogger.info("#{__MODULE__}", "Stopping the DeploymentPipeline")
    # Second stop the DeploymentPipeline
    Redis.publish_async("ingest_channel", %{stop_deployment_pipeline: "deployment"})

    ensure_deployment_pipeline_stopped()

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting the Deployment Topic: #{Config.deployment_topic()}"
    )

    # Third delete all data for the delployment topic
    delete_topic_result = Kafka.Api.Topic.delete_topic(Config.deployment_topic())

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
    )

    # Fourth reset all the data for each event_definition
    DeleteEventDefinitionsAndTopicsWorker.perform(%{
      "event_definition_ids" => [],
      "hard_delete" => true,
      "delete_topics" => true
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
      case DeploymentPipeline.deployment_pipeline_running?() or
             not DeploymentPipeline.deployment_pipeline_finished_processing?() do
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
    try do
      Exq.Api.clear_processes(Exq.Api)
      Exq.Api.clear_failed(Exq.Api)
      Exq.Api.clear_retries(Exq.Api)
      Exq.Api.clear_scheduled(Exq.Api)

      case Exq.Api.queues(Exq.Api) do
        {:ok, queues} ->
          Enum.each(queues, fn queue_name ->
            Exq.unsubscribe(Exq, queue_name)
            Exq.Api.remove_queue(Exq.Api, queue_name)
          end)

        _ ->
          nil
      end
    rescue
      e ->
        CogyntLogger.error("#{__MODULE__}", "Failed to Reset JobQ data. Error: #{e}")
    end

    Redis.key_delete("dpcgid")
    Redis.key_delete("dpmi")
    Redis.key_delete("fdpm")
    Redis.hash_delete("crw", Config.deployment_topic())

    CogyntLogger.info("#{__MODULE__}", "Starting the DeploymentPipeline")

    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
