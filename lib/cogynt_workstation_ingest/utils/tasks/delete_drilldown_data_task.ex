defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_drilldown_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias Models.Deployments.Deployment
  alias CogyntWorkstationIngest.Supervisors.{ConsumerGroupSupervisor, TaskSupervisor}
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  # alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstationIngest.Broadway.DrilldownProducer

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(%{delete_topics: delete_topics, deleting_deployments: deleting_deployments}) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_drilldown_data_task with option delete_topics: #{delete_topics}, deleting_deployments: #{
        deleting_deployments
      }"
    )

    delete_drilldown_data(deleting_deployments, delete_topics)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_drilldown_data(deleting_deployments, delete_topics) do
    deployments = DeploymentsContext.list_deployments()

    Enum.each(deployments, fn %Deployment{id: id} = deployment ->
      CogyntLogger.info("#{__MODULE__}", "Stoping the Drilldown ConsumerGroup's")
      ConsumerGroupSupervisor.stop_child(:drilldown, deployment)

      if delete_topics do
        {:ok, uris} = DeploymentsContext.get_kafka_brokers(id)

        hash_string = Integer.to_string(:erlang.phash2(uris))
        worker_name = String.to_atom("drilldown" <> hash_string)

        CogyntLogger.info(
          "#{__MODULE__}",
          "Deleting the Drilldown Topics. #{Config.topic_sols()}, #{Config.topic_sol_events()}. For KafkaWorker: #{
            worker_name
          }, Brokers: #{inspect(uris)}"
        )

        # Delete topics for worker
        delete_topic_result =
          KafkaEx.delete_topics([Config.topic_sols(), Config.topic_sol_events()],
            worker_name: worker_name
          )

        CogyntLogger.info(
          "#{__MODULE__}",
          "Deleted Drilldown Topics result: #{inspect(delete_topic_result, pretty: true)}"
        )
      end
    end)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Sleeping for 10 seconds to let any last min messages from consumer be added to the queue...."
    )

    Process.sleep(10_000)

    CogyntLogger.info("#{__MODULE__}", "Starting resetting of drilldown data")

    if deleting_deployments do
      # Do not start the DrilldownConsumers since their deployments are going
      # to be removed. They will be created when new deployments are created
      reset_drilldown([])
      # trigger delete deployment task
      TaskSupervisor.start_child(%{delete_deployment_data: delete_topics})
    else
      reset_drilldown(deployments)
    end
  end

  defp reset_drilldown(deployments, counter \\ 0) do
    if counter >= 6 do
      reset_cached_data()
      Process.sleep(2000)

      Enum.each(deployments, fn deployment ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Starting Drilldown ConsumerGroup for DeploymentID: #{deployment.id}"
        )

        ConsumerGroupSupervisor.start_child(:drilldown, deployment)
      end)
    else
      DrilldownProducer.flush_queue()

      case finished_processing?() do
        {:ok, true} ->
          reset_cached_data()
          Process.sleep(2000)

          Enum.each(deployments, fn deployment ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Starting Drilldown ConsumerGroup for DeploymentID: #{deployment.id}"
            )

            ConsumerGroupSupervisor.start_child(:drilldown, deployment)
          end)

        _ ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "DrilldownData still in pipeline, flushing and trying again in 10 seconds..."
          )

          Process.sleep(10_000)
          reset_drilldown(deployments, counter + 1)
      end
    end
  end

  defp reset_cached_data() do
    DrilldownProducer.flush_queue()
    Redis.key_delete("drilldown_message_info")
    Redis.key_delete("dcgid")
    Redis.hash_increment_by("drilldown_message_info", "tmp", 0)
    DrilldownContext.hard_delete_template_solutions_data()
    # DrilldownCache.reset_state()
  end

  defp finished_processing?() do
    case Redis.key_exists?("drilldown_message_info") do
      {:ok, false} ->
        {:error, :key_does_not_exist}

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("drilldown_message_info", "tmc")
        {:ok, tmp} = Redis.hash_get("drilldown_message_info", "tmp")

        {:ok, String.to_integer(tmp) >= String.to_integer(tmc)}
    end
  end
end
