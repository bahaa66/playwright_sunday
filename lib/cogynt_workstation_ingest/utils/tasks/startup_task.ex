defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Drilldown.DrilldownSinkConnector

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    start_event_type_pipelines()
    start_deployment_pipeline()
    start_drilldown_connector()
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

  # TODO: eventually need to do this for every deployment target. Connector needs
  # to support multiple kafka brokers
  defp start_drilldown_connector(count \\ 0) do
    if count <= 5 do
      # make sure rest port for connect is running
      case DrilldownSinkConnector.kafka_connect_health() do
        {:error, reason} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to connect to KafkaConnect because:#{reason}. Sleeping and trying again."
          )

          Process.sleep(25000)
          start_drilldown_connector(count + 1)

        {:ok, body} ->
          CogyntLogger.info("#{__MODULE__}", "Connected to Kafka Connect. #{inspect(body)}")
          # check if the connectors are present, then create or update
          DrilldownSinkConnector.create_or_update()
      end
    else
      CogyntLogger.error(
        "#{__MODULE__}",
        "Failed to connect to Kafka connect and exceeded retry attempts"
      )
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
