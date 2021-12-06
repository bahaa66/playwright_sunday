defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    start_event_type_pipelines()
    start_deployment_pipeline()

    DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
      Config.template_solutions_topic()
    )

    DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
      Config.template_solution_events_topic()
    )

    ExqHelpers.resubscribe_to_all_queues()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_pipelines() do
    event_definitions =
      EventsContext.query_event_definitions(%{
        filter: %{
          active: true
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
end
