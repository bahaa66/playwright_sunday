defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDeploymentDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_deployment_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Broadway.DeploymentPipeline
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias CogyntWorkstationIngest.Utils.Tasks.{
    DeleteDrilldownDataTask,
    DeleteEventDefinitionsAndTopicsTask
  }

  alias CogyntWorkstationIngest.Config

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(_arg) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_deployment_data_task"
    )

    delete_deployment_data()
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_deployment_data() do
    # First reset all DrilldownData passing true to delete topic data
    DeleteDrilldownDataTask.run(true)

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
    DeleteEventDefinitionsAndTopicsTask.run(%{
      event_definition_ids: [],
      hard_delete: true,
      delete_topics: true
    })

    # Finally reset all the deployment data
    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data()
  end

  def ensure_deployment_pipeline_stopped() do
    case DeploymentPipeline.deployment_pipeline_running?() or
           not DeploymentPipeline.deployment_pipeline_finished_processing?() do
      true ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "DeploymentPipeline still running... waiting for it to shutdown before resetting data"
        )

        Process.sleep(500)
        ensure_deployment_pipeline_stopped()

      false ->
        nil
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

    case Redis.keys_by_pattern("exq:*") do
      {:ok, []} ->
        nil

      {:ok, job_q_keys} ->
        Redis.key_delete_pipeline(job_q_keys)
    end

    Redis.key_delete("dpcgid")
    Redis.key_delete("dpmi")
    Redis.key_delete("fdpm")
    Redis.hash_delete("crw", Config.deployment_topic())

    CogyntLogger.info("#{__MODULE__}", "Starting the DeploymentPipeline")

    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
