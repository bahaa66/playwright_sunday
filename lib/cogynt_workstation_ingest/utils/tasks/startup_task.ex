defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    case Redis.hash_set_if_not_exists("elastic_lock", "event", "locked") do
      {:ok, 0} ->
        # LockKey Exists
        CogyntLogger.info(
          "#{__MODULE__}",
          "Redis hashkey elastic_lock event exists. Skipping Elasticsearch startup"
        )

      {:ok, 1} ->
        # LockKey does not exist
        case create_elastic_deps() do
          {:ok, _} ->
            start_event_type_pipelines()
            start_deployment_pipeline()

            DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
              Config.template_solutions_topic()
            )

            DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
              Config.template_solution_events_topic()
            )

            ExqHelpers.resubscribe_to_all_queues()
            Redis.hash_delete("elastic_lock", "event")

          {:error, _} ->
            Redis.hash_delete("elastic_lock", "event")
            raise "StartUp Task Failed, Failed to Create/Reindex Elasticsearch"
        end
    end
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

  defp create_elastic_deps() do
    CogyntLogger.info("#{__MODULE__}", "Creating Elastic Indexes...")

    with {:ok, false} <- ElasticApi.index_exists?(Config.event_index_alias()) do
      ElasticApi.create_index(Config.event_index_alias())

      CogyntLogger.info(
        "#{__MODULE__}",
        "The Index: #{Config.event_index_alias()} for CogyntWorkstation has been created."
      )

      CogyntLogger.info("#{__MODULE__}", "Indexes complete..")
      {:ok, :success}
    else
      {:ok, true} ->
        ElasticApi.check_to_reindex()
        CogyntLogger.info("#{__MODULE__}", "Reindexing Check complete..")
        {:ok, :success}

      {:error, %Elasticsearch.Exception{raw: %{"error" => error}}} ->
        reason = Map.get(error, "reason")

        CogyntLogger.info(
          "#{__MODULE__}",
          "Failed to Create #{Config.event_index_alias()} Index: #{reason}"
        )

        {:error, reason}

      {:error, error} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Failed to Create #{Config.event_index_alias()} Index: #{inspect(error, pretty: true)}"
        )

        {:error, error}
    end
  end
end
