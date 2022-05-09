defmodule CogyntWorkstationIngest.Servers.BroadwayProducerMonitor do
  @moduledoc """
  """
  use GenServer
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(producer_name, event_definition) do
    GenServer.cast(__MODULE__, {:monitor, producer_name, event_definition})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, producer_name, event_definition}, state) do
    pid =
      Broadway.producer_names(producer_name)
      |> List.first()
      |> Process.whereis()

    unless is_nil(pid) do
      Process.monitor(pid)
    end

    new_state = Map.put(state, pid, event_definition)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, :process, pid,
         {%RuntimeError{
            message: failure_message
          }, _}},
        state
      ) do
    unless !String.contains?(failure_message, ":unknown_topic_or_partition") do
      unless !Map.has_key?(state, pid) do
        event_definition = Map.get(state, pid)
        # Shut Down Consumer
        ConsumerStateManager.manage_request(%{shutdown_consumer: event_definition})
        # Wait till Shutsdown
        ensure_pipeline_shutdown(event_definition)
        # Put it in the start queue again
        ConsumerStateManager.manage_request(%{start_consumer: event_definition})
      end
    end

    {:noreply, Map.delete(state, pid)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, Map.delete(state, pid)}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp ensure_pipeline_shutdown(event_definition, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_pipeline_shutdown/1 exceeded number of attempts (30) Moving forward with BroadwayProducerMonitor"
      )
    else
      {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

      case EventPipeline.pipeline_started?(event_definition.id) or
             not EventPipeline.pipeline_finished_processing?(event_definition.id) or
             consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition.title} still running... waiting 5000 ms for it to shutdown before resetting data"
          )

          Process.sleep(5000)
          ensure_pipeline_shutdown(event_definition, count + 1)

        false ->
          nil
      end
    end
  end
end
