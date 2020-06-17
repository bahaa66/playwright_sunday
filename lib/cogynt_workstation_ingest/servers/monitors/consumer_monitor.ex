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
  alias CogyntWorkstationIngest.Servers.ConsumerStateManager

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
  def handle_cast({:monitor, pid, event_definition_id, topic, type}, state) do
    Process.monitor(pid)

    new_state =
      Map.put(state, pid, %{event_definition_id: event_definition_id, topic: topic, type: type})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    %{event_definition_id: event_definition_id, topic: topic, type: type} = Map.get(state, pid)

    %{status: status} = ConsumerStateManager.get_consumer_state(event_definition_id)

    case Producer.is_processing?(event_definition_id, type) do
      true ->
        check_consumer_state(
          event_definition_id,
          topic,
          status,
          ConsumerStatusTypeEnum.status()[:paused_and_processing]
        )

      false ->
        check_consumer_state(
          event_definition_id,
          topic,
          status,
          ConsumerStatusTypeEnum.status()[:paused_and_finished]
        )
    end

    new_state = Map.delete(state, pid)
    {:noreply, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp check_consumer_state(id, topic, status, new_status) do
    cond do
      status == new_status ->
        CogyntClient.publish_consumer_status(
          id,
          topic,
          new_status
        )

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        CogyntClient.publish_consumer_status(
          id,
          topic,
          new_status
        )

      status == ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
        CogyntClient.publish_consumer_status(
          id,
          topic,
          new_status
        )

      true ->
        ConsumerStateManager.update_consumer_state(id,
          topic: topic,
          status: new_status
        )

        CogyntClient.publish_consumer_status(
          id,
          topic,
          new_status
        )
    end
  end
end
