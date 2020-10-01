defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_drilldown_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias Models.Deployments.Deployment
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
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
    deployments = DeploymentsContext.list_deployments()

    Enum.each(deployments, fn %Deployment{id: id} = deployment ->
      CogyntLogger.info("#{__MODULE__}", "Stoping the Drilldown ConsumerGroup's")
      ConsumerGroupSupervisor.stop_child(:drilldown, deployment)

      {:ok, brokers} = DeploymentsContext.get_kafka_brokers(id)

      hashed_brokers = Integer.to_string(:erlang.phash2(brokers))

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

      reset_drilldown(deployment, hashed_brokers)
    end)
  end

  defp reset_drilldown(deployment, hashed_brokers, counter \\ 0) do
    if counter >= 6 do
      reset_cached_data()

      CogyntLogger.info(
        "#{__MODULE__}",
        "Starting Drilldown ConsumerGroup for DeploymentID: #{deployment.id}"
      )

      ConsumerGroupSupervisor.start_child(:drilldown, deployment)
    else
      case finished_processing?(hashed_brokers) do
        {:ok, true} ->
          reset_cached_data()

          CogyntLogger.info(
            "#{__MODULE__}",
            "Starting Drilldown ConsumerGroup for DeploymentID: #{deployment.id}"
          )

          ConsumerGroupSupervisor.start_child(:drilldown, deployment)

        _ ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "DrilldownData still in pipeline, flushing and trying again in 5 seconds..."
          )

          Process.sleep(5_000)
          reset_drilldown(deployment, hashed_brokers, counter + 1)
      end
    end
  end

  defp reset_cached_data() do
    Redis.key_delete("dcgid")

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

  defp finished_processing?(hashed_brokers) do
    consumer_group_id =
      case Redis.hash_get("dcgid", "Drilldown-#{hashed_brokers}") do
        {:ok, nil} ->
          ""

        {:ok, consumer_group_id} ->
          "Drilldown-#{hashed_brokers}" <> "-" <> consumer_group_id
      end

    if consumer_group_id == nil do
      {:ok, true}
    else
      case Redis.key_exists?("dmi:#{consumer_group_id}") do
        {:ok, false} ->
          {:ok, true}

        {:ok, true} ->
          {:ok, tmc} = Redis.hash_get("dmi:#{consumer_group_id}", "tmc")
          {:ok, tmp} = Redis.hash_get("dmi:#{consumer_group_id}", "tmp")

          {:ok, String.to_integer(tmp) >= String.to_integer(tmc)}
      end
    end
  end
end
