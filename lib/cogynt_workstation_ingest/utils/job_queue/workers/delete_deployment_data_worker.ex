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

  @dev_delete_queue "DevDelete"

  def perform(
        %{
          "event_definition_hash_ids" => event_definition_hash_ids,
          "delete_topics" => delete_topics_for_deployments
        } = args
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "RUNNING DELETE DEPLOYMENT DATA WORKER. Args: #{inspect(args, pretty: true)}"
    )

    # First reset all DrilldownData passing true to delete topic data
    ExqHelpers.enqueue(
      @dev_delete_queue,
      DeleteDrilldownDataWorker,
      delete_topics_for_deployments
    )

    # Second shutdown the DeploymentPipeline
    Redis.publish_async("ingest_channel", %{shutdown_deployment_pipeline: "deployment"})

    ensure_deployment_pipeline_stopped()

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
      ExqHelpers.enqueue(@dev_delete_queue, DeleteEventDefinitionsAndTopicsWorker, %{
        "event_definition_hash_id" => event_definition_hash_id,
        "delete_topics" => delete_topics_for_deployments
      })
    end)

    # Allow the EXQ tasks time to queue there jobs
    Process.sleep(10000)

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
        "ensure_deployment_pipeline_stopped/1 exceeded number of attempts (30). Moving forward with DeleteDeploymentData"
      )
    else
      case DeploymentPipeline.pipeline_running?() or
             not DeploymentPipeline.pipeline_finished_processing?() do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DeploymentPipeline still running... waiting 1000 ms for it to shutdown before resetting data"
          )

          Process.sleep(1000)
          ensure_deployment_pipeline_stopped(count + 1)

        false ->
          nil
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
      case Exq.Api.queue_size(Exq.Api, @dev_delete_queue) do
        {:ok, jobs} ->
          if jobs <= 1 do
            nil
          else
            CogyntLogger.info(
              "#{__MODULE__}",
              "DevDelete Queued Tasks still running... waiting 120000 ms for it to shutdown before resetting data"
            )

            # Will retry every 2 mins for a max retry limit of 30. Will wait for a max of 1 before
            # Finishing the Deleting of the remaining Deployment data
            Process.sleep(120_000)
            ensure_enqueued_queue_tasks_finished(count + 1)
          end

        _ ->
          nil
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

    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
