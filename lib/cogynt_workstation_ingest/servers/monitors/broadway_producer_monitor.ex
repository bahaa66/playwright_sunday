defmodule CogyntWorkstationIngest.Servers.BroadwayProducerMonitor do
  @moduledoc """
  """
  use GenServer

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
        Redis.publish_async("ingest_channel", %{
          shutdown_consumer: Map.get(state, pid)
        })
      end
    end

    {:noreply, Map.delete(state, pid)}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, Map.delete(state, pid)}
  end
end
