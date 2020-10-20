defmodule CogyntWorkstationIngest.Servers.Workers.ConsumerRetryWorker do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
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
      {:ok, results} ->
        Map.keys(results)
        |> Enum.each(fn key ->
          cond do
            Map.get(results, key) == "et" ->
              CogyntLogger.info("#{__MODULE__}", "Retrying to start consumer for id: #{key}")
              start_event_type_consumers(key)

            Map.get(results, key) == "dp" ->
              CogyntLogger.info("#{__MODULE__}", "Retrying to start consumer for topic: #{key}")
              start_deployment_consumer(key)

            true ->
              CogyntLogger.warn("#{__MODULE__}", "Unknown key found in CRW Redis hash")
              Redis.hash_delete("crw", key)
          end
        end)

      {:error, _} ->
        CogyntLogger.error("#{__MODULE__}", "Failed to fetch CRW Redis hash")
    end

    {:noreply, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_consumers(event_definition_id) do
    with %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(event_definition_id),
         true <- is_nil(event_definition.deleted_at),
         true <- event_definition.active do
      ed = EventsContext.remove_event_definition_virtual_fields(event_definition)
      Redis.hash_delete("crw", event_definition_id)
      ConsumerStateManager.manage_request(%{start_consumer: ed})
    else
      false ->
        Redis.hash_delete("crw", event_definition_id)
    end
  end

  defp start_deployment_consumer(topic) do
    case ConsumerGroupSupervisor.start_child(topic) do
      {:error, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Deployment topic DNE. Will retry to create consumer..."
        )

      _ ->
        Redis.hash_delete("crw", topic)
        CogyntLogger.info("#{__MODULE__}", "Started Deployment Consumer")
    end
  end
end
