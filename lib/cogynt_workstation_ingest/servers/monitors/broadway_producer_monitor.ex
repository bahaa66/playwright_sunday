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
    |> IO.inspect(label: "**************** PRODUCER NAMES")
    |> Enum.each(fn producer_name ->
      pid = Process.whereis(producer_name)

      unless is_nil(pid) do
        IO.inspect(pid, label: "**************** Monitoring PID")
        Process.monitor(pid)
      end
    end)

    new_state = Map.put(state, producer_name, %{event_definition: event_definition})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({status, _ref, _, _pid, reason}, state) do
    IO.inspect(reason, label: "***************** CONSUMER FAILED WITH REASON")
    IO.inspect(status, label: "***************** CONSUMER STATUS")
    {:noreply, state}
  end
end
