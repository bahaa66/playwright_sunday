defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDeploymentDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Broadway.DeploymentPipeline
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Events.EventsContext

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Config

  def perform(%{
        "event_definition_hash_ids" => event_definition_hash_ids,
        "delete_topics" => delete_topics_for_deployments
      }) do
    # First reset all DrilldownData passing true to delete topic data
    ExqHelpers.enqueue("DevDelete", DeleteDrilldownDataWorker, delete_topics_for_deployments)

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
    Enum.each(event_definition_hash_ids, fn event_definition_hash_id ->
      ExqHelpers.enqueue("DevDelete", DeleteEventDefinitionsAndTopicsWorker, %{
        "event_definition_hash_id" => event_definition_hash_id,
        "delete_topics" => delete_topics_for_deployments
      })
    end)

    # Finally reset all the deployment data
    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    ensure_enqueued_queue_tasks_finished()
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

  defp ensure_enqueued_queue_tasks_finished(count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_enqueued_queue_tasks_finished/1 exceeded number of attempts (30). Moving forward with DeleteDeploymentData"
      )
    else
      case Redis.get("dd") do
        {:ok, nil} ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DevDelete Queue tasks finished. Proceed with DeleteDeploymentData"
          )

        {:ok, values} ->
          if Enum.count(values) <= 1 do
            CogyntLogger.info(
              "#{__MODULE__}",
              "DevDelete Queue tasks finished. Proceed with DeleteDeploymentData"
            )
          else
            CogyntLogger.info(
              "#{__MODULE__}",
              "DevDelete Queued Tasks still running... waiting for it to shutdown before resetting data"
            )

            # Will retry every 2 mins for a max retry limit of 30. Will wait for a max of 1 before
            # Finishing the Deleting of the remaining Deployment data
            Process.sleep(120_000)
            ensure_enqueued_queue_tasks_finished(count + 1)
          end

        _ ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DevDelete Queue tasks finished. Proceed with DeleteDeploymentData"
          )
      end
    end
  end

  defp reset_deployment_data() do
    # TODO: Add `deployments` table in the list of truncate_all_tables function
    # and remove `hard_delete_deployments()`
    DeploymentsContext.hard_delete_deployments()
    EventsContext.truncate_all_tables()
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
