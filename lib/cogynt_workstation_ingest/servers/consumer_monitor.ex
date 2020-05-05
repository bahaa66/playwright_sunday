defmodule CogyntWorkstationIngest.Servers.ConsumerMonitor do
  @moduledoc """
  Module that monitors the PIDs of consumers. When a consumers
  is created it will start monitoring the PID and store it in the state.
  When a consumer is shutdown it will push the consumers status to cogynt-otp
  and remove the PID from the state.
  """
  use GenServer
  alias CogyntWorkstationIngest.Broadway.Producer
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias Models.Enums.ConsumerStatusTypeEnum

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid, id, topic, type) do
    GenServer.cast(__MODULE__, {:monitor, pid, id, topic, type})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid, id, topic, type}, state) do
    Process.monitor(pid)

    new_state = Map.put(state, pid, %{id: id, topic: topic, type: type})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    %{id: id, topic: topic, type: type} = Map.get(state, pid)

    case Producer.is_processing?(id, type) do
      true ->
        CogyntClient.publish_consumer_status(
          id,
          topic,
          ConsumerStatusTypeEnum.status()[:paused_and_processing]
        )

      false ->
        CogyntClient.publish_consumer_status(
          id,
          topic,
          ConsumerStatusTypeEnum.status()[:paused_and_finished]
        )
    end

    new_state = Map.delete(state, pid)
    {:noreply, new_state}
  end
end
