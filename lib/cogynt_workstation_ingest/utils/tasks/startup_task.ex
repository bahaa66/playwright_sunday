defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    start_event_type_pipelines()
    start_deployment_pipeline()
    start_drilldown_pipelines()
    resubscribe_to_job_queues()
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

  defp resubscribe_to_job_queues() do
    case Exq.Api.queues(Exq.Api) do
      {:ok, queues} ->
        Enum.each(queues, fn queue_name ->
          if queue_name == "DevDelete" do
            Exq.subscribe(Exq, queue_name, 1)
          else
            Exq.subscribe(Exq, queue_name, 5)
          end
        end)

      _ ->
        nil
    end
  end
end
