defmodule CogyntWorkstationIngest.Broadway.DeploymentProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DrilldownPipeline.
  """

  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  alias Models.Deployments.Deployment
  alias Models.Enums.DeploymentStatusTypeEnum
  alias Models.Events.EventDefinition
  alias Broadway.Message

  @doc """
  process_deployment_message/1
  """
  def process_deployment_message(%Message{data: nil} = message) do
    CogyntLogger.error("#{__MODULE__}", "Message data is nil. No data to process")
    message
  end

  def process_deployment_message(
        %Message{data: %{deployment_message: deployment_message}} = message
      ) do
    case Map.get(deployment_message, :object_type, nil) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "`object_type` key is missing from Deployment Stream message. #{
            inspect(deployment_message, pretty: true)
          }"
        )

        message

      "event_type" ->
        # Temp Store id as `authoring_event_definition_id` until field can be removed
        Map.put(deployment_message, :authoring_event_definition_id, deployment_message.id)
        |> Map.put(:topic, deployment_message.filter)
        |> Map.put(:title, deployment_message.name)
        |> Map.put(
          :manual_actions,
          Map.get(deployment_message, :manualActions, nil)
        )
        |> Map.put_new_lazy(:event_type, fn ->
          if is_nil(deployment_message.dsType) do
            :none
          else
            deployment_message.dsType
          end
        end)
        |> Map.drop([:id])
        |> EventsContext.upsert_event_definition()

        message

      "deployment" ->
        # Upsert Deployments
        {:ok, %Deployment{} = deployment} =
          DeploymentsContext.upsert_deployment(deployment_message)

        # Fetch all event_definitions that exists and are assosciated with
        # the deployment_id
        current_event_definitions =
          EventsContext.query_event_definitions(
            filter: %{
              deployment_id: deployment_message.id
            }
          )

        # If any of these event_definition_id are not in the list of event_definition_ids
        # passed in the deployment message, mark them as inactive and shut off the consumer.
        # If they are in the list make sure to update the DeploymentStatus if it needs to be changed
        Enum.each(current_event_definitions, fn %EventDefinition{
                                                  deployment_status: deployment_status,
                                                  id: event_definition_id,
                                                  authoring_event_definition_id:
                                                    authoring_event_definition_id
                                                } = current_event_definition ->
          case Enum.member?(
                 deployment_message.event_type_ids,
                 authoring_event_definition_id
               ) do
            true ->
              if deployment_status == DeploymentStatusTypeEnum.status()[:inactive] or
                   deployment_status == DeploymentStatusTypeEnum.status()[:not_deployed] do
                EventsContext.update_event_definition(current_event_definition, %{
                  deployment_status: DeploymentStatusTypeEnum.status()[:active]
                })
              end

            false ->
              EventsContext.update_event_definition(current_event_definition, %{
                active: false,
                deployment_status: DeploymentStatusTypeEnum.status()[:inactive]
              })

              ConsumerStateManager.manage_request(%{
                stop_consumer: event_definition_id
              })
          end
        end)

        # Start Drilldown Consumer for Deployment
        if not ConsumerGroupSupervisor.drilldown_consumer_running?(deployment) do
          CogyntLogger.info(
            "#{__MODULE__}",
            "Starting Drilldown ConsumerGroup for deplpoyment_id: #{deployment.id}"
          )

          ConsumerGroupSupervisor.start_child(:drilldown, deployment)
        end

        message

      _ ->
        message
    end
  end
end
