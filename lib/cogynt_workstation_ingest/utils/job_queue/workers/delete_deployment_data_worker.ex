defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDeploymentDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Broadway.DeploymentPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Config

  def perform(delete_topics_for_deployments) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "RUNNING DELETE DEPLOYMENT DATA WORKER. delete_topics: #{delete_topics_for_deployments}"
    )

    # First shutdown the DeploymentPipeline
    Redis.publish_async("ingest_channel", %{shutdown_deployment_pipeline: "deployment"})

    ensure_deployment_pipeline_stopped()

    # Second delete all data for the delployment topic
    if delete_topics_for_deployments do
      delete_topic_result = Kafka.Api.Topic.delete_topic(Config.deployment_topic())

      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    end

    # Finally reset all the deployment data
    ensure_enqueued_queue_tasks_finished()
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
      case Redis.set_length("dd") do
        {:ok, count} ->
          if count <= 1 do
            nil
          else
            CogyntLogger.info(
              "#{__MODULE__}",
              "DevDelete Queued Tasks still running... waiting 10000 ms for it to shutdown before resetting data"
            )

            # Will retry every 2 mins for a max retry limit of 30. Will wait for a max of 1 before
            # Finishing the Deleting of the remaining Deployment data
            Process.sleep(10_000)
            ensure_enqueued_queue_tasks_finished(count + 1)
          end

        _ ->
          nil
      end
    end
  end

  defp reset_deployment_data() do
    EventsContext.truncate_all_tables()
    NotificationsContext.delete_notification_settings()
    EventsContext.delete_event_definitions()

    # Reset all JobQ Info
    ExqHelpers.flush_all()

    Redis.key_delete("dpcgid")
    Redis.key_delete("dpmi")
    Redis.key_delete("fdpm")
    Redis.hash_delete("crw", Config.deployment_topic())

    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
