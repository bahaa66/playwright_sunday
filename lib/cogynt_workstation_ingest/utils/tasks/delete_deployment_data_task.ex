defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDeploymentDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_deployment_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.{ConsumerGroupSupervisor, TaskSupervisor}
  alias CogyntWorkstationIngest.Servers.Caches.DeploymentConsumerRetryCache
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask

  alias Models.Events.EventDefinition

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run() do
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
    # Reset all Drilldowndata first
    DeleteDrilldownDataTask.run(true)

    CogyntLogger.info("#{__MODULE__}", "Stoping the Deployment ConsumerGroup")
    ConsumerGroupSupervisor.stop_child(:deployment)

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
      "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
    )

    event_definitions = EventsContext.list_event_definitions()

    Enum.each(event_definitions, fn %EventDefinition{
                                      id: id,
                                      topic: topic,
                                      deployment_id: deployment_id
                                    } ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Stoping ConsumerGroup for #{topic}"
      )

      # TODO: Need to check consumer state ?
      ConsumerStateManager.manage_request(%{stop_consumer: id})

      CogyntLogger.info("#{__MODULE__}", "Deleting Kakfa topic: #{topic}")
      worker_name = String.to_atom("deployment#{deployment_id}")
      KafkaEx.delete_topics([topic], worker_name: worker_name)
    end)

    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data()
  end

  defp reset_deployment_data() do
    event_definitions = EventsContext.list_event_definitions()

    event_definition_ids =
      Enum.reduce(event_definitions, [], fn event_definition, acc ->
        acc ++ [event_definition.id]
      end)

    TaskSupervisor.start_child(%{
      delete_event_definitions_and_topics: %{
        event_definition_ids: event_definition_ids,
        hard_delete: true,
        delete_topics: true
      }
    })

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
