defmodule CogyntWorkstationIngest.Servers.Workers.ConsumerRetryWorker do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    Process.send_after(__MODULE__, :consumer_retry, Config.consumer_retry_retry_timer())
    {:ok, %{}}
  end

  @impl true
  def handle_info(:consumer_retry, state) do
    Process.send_after(__MODULE__, :consumer_retry, Config.consumer_retry_retry_timer())

    case Redis.hash_get_all("crw") do
      {:ok, result} ->
        Map.keys(result)
        |> Enum.each(fn key ->
          cond do
            Map.get(result, key) == "et" ->
              CogyntLogger.info("#{__MODULE__}", "Retrying to start consumer for id: #{key}")
              start_event_type_consumers(key)

            Map.get(result, key) == "dp" ->
              CogyntLogger.info("#{__MODULE__}", "Retrying to start consumer for topic: #{key}")
              start_deployment_consumer(key)

            true ->
              CogyntLogger.warn("#{__MODULE__}", "Unknown key found in CRW Redis hash")
              Redis.hash_delete("crw", key)
          end
        end)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to fetch CRW Redis hash. Reason: #{inspect(reason)}"
        )
    end

    {:noreply, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_consumers(event_definition_id) do
    with %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(event_definition_id),
         true <- event_definition.active do
      Redis.hash_delete("crw", event_definition_id)

      Redis.publish_async("ingest_channel", %{
        start_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    else
      _ ->
        Redis.hash_delete("crw", event_definition_id)
    end
  end

  defp start_deployment_consumer(topic) do
    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: topic})
  end
end
