defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers
  alias CogyntWorkstationIngest.Utils.PinotUtils

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    IO.inspect(node(), label: "NODE NAME")
    event_definitions = EventsContext.query_event_definitions(%{})
    start_event_type_pipelines(event_definitions)
    start_deployment_pipeline()

    if Config.drilldown_enabled?() do
      PinotUtils.create_schema_and_table(Config.template_solution_events_topic())
      |> case do
        :ok -> nil
        {:error, error} -> CogyntLogger.error("#{__MODULE__}", error)
      end
    end

    ExqHelpers.resubscribe_to_all_queues()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_pipelines(event_definitions) do
    event_definitions = Enum.filter(event_definitions, &Map.get(&1, :active, false))

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
