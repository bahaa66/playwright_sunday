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
    start_template_solutions_druid_supervisor()
    start_template_solution_events_druid_supervisor()
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

  defp start_template_solutions_druid_supervisor() do
    Redis.publish_async("ingest_channel", %{
      start_druid_supervisor: %{
        supervisor_id: Config.template_solutions_topic(),
        schema: :avro,
        schema_registry_url: Config.schema_registry_url(),
        brokers:
          Config.kafka_brokers()
          |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
          |> Enum.join(","),
        dimensions_spec: %{
          dimensions: [
            "id",
            "templateTypeName",
            "templateTypeId",
            "retracted"
          ]
        },
        name: Config.template_solutions_topic()
      }
    })
  end

  defp start_template_solution_events_druid_supervisor() do
    Redis.publish_async("ingest_channel", %{
      start_druid_supervisor: %{
        supervisor_id: Config.template_solution_events_topic(),
        schema: :avro,
        schema_registry_url: Config.schema_registry_url(),
        brokers:
          Config.kafka_brokers()
          |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
          |> Enum.join(","),
        dimensions_spec: %{
          dimensions: [
            "id",
            "templateTypeName",
            "templateTypeId",
            "event",
            "aid",
            "assertionName"
          ]
        },
        name: Config.template_solution_events_topic()
      }
    })
  end
end
