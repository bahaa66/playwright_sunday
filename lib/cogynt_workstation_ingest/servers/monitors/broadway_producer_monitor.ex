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
    Broadway.producer_names(producer_name)
    |> Enum.each(fn producer_name ->
      pid = Process.whereis(producer_name)

      unless is_nil(pid) do
        Process.monitor(pid)
      end
    end)

    new_state = Map.put(state, producer_name, %{event_definition: event_definition})

    {:noreply, new_state}
  end

  @impl true
  def handle_info(
        {:DOWN, _ref, _, pid,
         {%RuntimeError{
            message: failure_message
          }, _}},
        state
      ) do
    unless !String.contains?(failure_message, "Reason: :unknown_topic_or_partition") do
      IO.inspect(Process.info(pid), label: "PROCESS INFO")
      # Redis.publish_async("ingest_channel", %{
      #   shutdown_consumer: orig_event_definition
      # })
    end

    {:noreply, state}
  end
end
