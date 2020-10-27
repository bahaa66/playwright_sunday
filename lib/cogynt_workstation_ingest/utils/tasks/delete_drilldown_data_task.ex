defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_drilldown_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias Models.Deployments.Deployment
  alias CogyntWorkstationIngest.Broadway.DrilldownPipeline
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(delete_drilldown_topics) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_drilldown_data_task with option delete_drilldown_topics: #{
        delete_drilldown_topics
      }"
    )

    delete_drilldown_data(delete_drilldown_topics)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_drilldown_data(delete_drilldown_topics) do
    # First fetch all deployment records
    deployments = DeploymentsContext.list_deployments()

    # Second for each deployment stop its corresponding DrilldownPipeline
    # and delete any topic data if the options specify to do so
    Enum.each(deployments, fn %Deployment{id: deployment_id} = deployment ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Stopping the DrilldownPipeline for with DeploymentId: #{deployment_id}"
      )

      Redis.publish_async("ingest_channel", %{stop_drilldown_pipeline: deployment_id})

      # make sure the drilldownPipeline has stopped before moving forward
      ensure_drilldown_pipeline_stopped(deployment)

      case DeploymentsContext.get_kafka_brokers(deployment_id) do
        {:error, :does_not_exist} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to fetch brokers for DeploymentId: #{deployment_id}"
          )

          # Third if delete_drilldown_topics is true delete the drilldown topics for the
          # kafka broker assosciated with the deployment_id
          if delete_drilldown_topics do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleting the Drilldown Topics. #{Config.template_solutions_topic()}, #{
                Config.template_solution_events_topic()
              }. Brokers: #{Config.kafka_brokers()}"
            )

            # Delete topics for worker
            delete_topic_result =
              Kafka.Api.Topic.delete_topics([
                Config.template_solutions_topic(),
                Config.template_solution_events_topic()
              ])

            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleted Drilldown Topics result: #{inspect(delete_topic_result, pretty: true)}"
            )
          end

          CogyntLogger.info("#{__MODULE__}", "Starting resetting of drilldown data")

          # Fourth reset all the drilldown data
          reset_drilldown_data()
          # Finally start the drilldownPipeline again
          Redis.publish_async("ingest_channel", %{start_drilldown_pipeline: deployment_id})

        {:ok, brokers} ->
          hashed_brokers = Integer.to_string(:erlang.phash2(brokers))
          # Third if delete_drilldown_topics is true delete the drilldown topics for the
          # kafka broker assosciated with the deployment_id
          if delete_drilldown_topics do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleting the Drilldown Topics. #{Config.template_solutions_topic()}, #{
                Config.template_solution_events_topic()
              }. Brokers: #{inspect(brokers)}"
            )

            # Delete topics for worker
            delete_topic_result =
              Kafka.Api.Topic.delete_topics(
                [Config.template_solutions_topic(), Config.template_solution_events_topic()],
                brokers
              )

            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleted Drilldown Topics result: #{inspect(delete_topic_result, pretty: true)}"
            )
          end

          CogyntLogger.info("#{__MODULE__}", "Starting resetting of drilldown data")

          # Fourth reset all the drilldown data
          reset_drilldown_data(hashed_brokers)
          # Finally start the drilldownPipeline again
          Redis.publish_async("ingest_channel", %{start_drilldown_pipeline: deployment_id})
      end
    end)
  end

  defp ensure_drilldown_pipeline_stopped(deployment) do
    case DrilldownPipeline.drilldown_pipeline_running?(deployment) or
           not DrilldownPipeline.drilldown_pipeline_finished_processing?(deployment) do
      true ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "DrilldownPipeline still running... waiting for it to shutdown before resetting data"
        )

        Process.sleep(500)
        ensure_drilldown_pipeline_stopped(deployment)

      false ->
        nil
    end
  end

  defp reset_drilldown_data(hashed_brokers \\ nil) do
    if is_nil(hashed_brokers) do
      Redis.hash_delete("dcgid", "Drilldown")
    else
      Redis.hash_delete("dcgid", "Drilldown-#{hashed_brokers}")
    end

    case Redis.keys_by_pattern("fdm:*") do
      {:ok, []} ->
        nil

      {:ok, failed_message_keys} ->
        Redis.key_delete_pipeline(failed_message_keys)
    end

    case Redis.keys_by_pattern("dmi:*") do
      {:ok, []} ->
        nil

      {:ok, message_info_keys} ->
        Redis.key_delete_pipeline(message_info_keys)
    end

    DrilldownContext.hard_delete_template_solutions_data()
  end
end
