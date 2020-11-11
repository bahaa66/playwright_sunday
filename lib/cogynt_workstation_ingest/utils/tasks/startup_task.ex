defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.DynamicTaskSupervisor
  alias Models.Enums.ConsumerStatusTypeEnum

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    start_event_type_pipelines()
    start_notification_tasks()
    start_deployment_pipeline()
    start_drilldown_pipelines()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_pipelines() do
    event_definitions =
      EventsContext.query_event_definitions(%{
        filter: %{
          active: true,
          deleted_at: nil
        }
      })

    Enum.each(event_definitions, fn event_definition ->
      Redis.publish_async("ingest_channel", %{
        start_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })

      CogyntLogger.info("#{__MODULE__}", "EventPipeline Started for Id: #{event_definition.id}")
    end)
  end

  defp start_notification_tasks() do
    event_definitions =
      EventsContext.query_event_definitions(%{
        filter: %{
          deleted_at: nil
        },
        select: [:id]
      })

    Enum.each(event_definitions, fn event_definition ->
      {:ok, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

      cond do
        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
          Enum.each(consumer_state.backfill_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing backfill notifications task: #{inspect(notification_setting_id)}"
            )

            DynamicTaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})
          end)

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
          Enum.each(consumer_state.update_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing update notifications task: #{inspect(notification_setting_id)}"
            )

            DynamicTaskSupervisor.start_child(%{update_notifications: notification_setting_id})
          end)

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
          Enum.each(consumer_state.delete_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing delete notifications task: #{inspect(notification_setting_id)}"
            )

            DynamicTaskSupervisor.start_child(%{
              delete_notification_setting: notification_setting_id
            })
          end)

        true ->
          # No tasks to trigger
          nil
      end
    end)
  end

  defp start_deployment_pipeline() do
    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end

  defp start_drilldown_pipelines() do
    case DeploymentsContext.list_deployments() do
      nil ->
        nil

      deployments ->
        Enum.each(deployments, fn deployment ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "Starting DrilldownPipeline for Deplpoyment_Id: #{deployment.id}"
          )

          Redis.publish_async("ingest_channel", %{start_drilldown_pipeline: deployment.id})
        end)
    end
  end
end
