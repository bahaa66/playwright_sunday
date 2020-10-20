defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDeploymentDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_deployment_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Supervisors.{ConsumerGroupSupervisor, DynamicTaskSupervisor}
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask

  alias Models.Events.EventDefinition

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

    CogyntLogger.info("#{__MODULE__}", "Stoping the Deployment ConsumerGroup")
    # Second stop the consumerGroup for the deployment topic
    ConsumerGroupSupervisor.stop_child(:deployment)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting the Deployment Topic: deployment"
    )

    # Third delete all data for the delployment topic
    delete_topic_result = Kafka.Api.Topic.delete_topic(Config.deployment_topic())

    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleted Deployment Topics result: #{inspect(delete_topic_result, pretty: true)}"
    )

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
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting Kakfa topic: #{topic}, deplpoyment_id: #{deployment_id}"
      )

      {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)

      Kafka.Api.Topic.delete_topic(topic, brokers)
    end)

    CogyntLogger.info("#{__MODULE__}", "Resetting Deployment Data")
    reset_deployment_data(event_definitions)
  end

  defp reset_deployment_data(event_definitions) do
    event_definition_ids = Enum.map(event_definitions, fn ed -> ed.id end)

    # Trigger the task to hard delete all event_definition_ids and its data
    DynamicTaskSupervisor.start_child(%{
      delete_event_definitions_and_topics: %{
        event_definition_ids: event_definition_ids,
        hard_delete: true,
        delete_topics: true
      }
    })

    DeploymentsContext.hard_delete_deployments()
    Redis.key_delete("dpcgid")
    Redis.key_delete("fdpm")
    Redis.hash_delete("crw", Config.deployment_topic())

    CogyntLogger.info("#{__MODULE__}", "Starting the Deployment ConsumerGroup")

    case ConsumerGroupSupervisor.start_child(:deployment) do
      {:error, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Deployment Topic DNE. Adding to retry cache. Will reconnect once topic is created"
        )

        Redis.hash_set_async("crw", Config.deployment_topic(), "dp")

      _ ->
        CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
    end
  end
end
