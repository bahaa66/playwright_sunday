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

  @deployment_worker_name :deployment_stream

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

    CogyntLogger.info("#{__MODULE__}", "Stoping the Deployment ConsumerGroup")
    # Second stop the consumerGroup for the deployment topic
    ConsumerGroupSupervisor.stop_child(:deployment)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting the Deployment Topic: deployment"
    )

    # Third delete all data for the delployment topic
    if Process.whereis(@deployment_worker_name) != nil do
      delete_topic_result =
        KafkaEx.delete_topics(["deployment"],
          worker_name: @deployment_worker_name
        )

      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    else
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleted Deployment Topics result: No PID associated to #{@deployment_worker_name}"
      )
    end

    # Fourth fetch all event_definitions, stop the consumers and delete the topic data
    event_definitions = EventsContext.list_event_definitions()

    Enum.each(event_definitions, fn %EventDefinition{
                                      id: event_definition_id,
                                      topic: topic,
                                      deployment_id: deployment_id
                                    } ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Stoping ConsumerGroup for #{topic}, Deployment_id: #{deployment_id}"
      )

      ConsumerStateManager.manage_request(%{stop_consumer: event_definition_id})

      # Delete topic data
      CogyntLogger.info("#{__MODULE__}", "Deleting Kakfa topic: #{topic}")
      worker_name = String.to_atom("deployment#{deployment_id}")

      if Process.whereis(worker_name) != nil do
        KafkaEx.delete_topics([topic], worker_name: worker_name)
      end
    end)

    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data(event_definitions)
  end

  defp reset_deployment_data(event_definitions) do
    event_definition_ids = Enum.map(event_definitions, fn ed -> ed.id end)

    # Trigger the task to hard delete all event_definition_ids and its data
    TaskSupervisor.start_child(%{
      delete_event_definitions_and_topics: %{
        event_definition_ids: event_definition_ids,
        hard_delete: true,
        delete_topics: true
      }
    })

    DeploymentsContext.hard_delete_deployments()
    Redis.key_delete("dpcgid")
    CogyntLogger.info("#{__MODULE__}", "Starting the Deployment ConsumerGroup")

    case ConsumerGroupSupervisor.start_child(:deployment) do
      {:error, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Deployment Topic DNE. Adding to retry cache. Will reconnect once topic is created"
        )

        DeploymentConsumerRetryCache.retry_consumer(:deployment)

      _ ->
        CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
    end
  end
end
