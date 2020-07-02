defmodule CogyntWorkstationIngest.Servers.ConsumerMonitor do
  @moduledoc """
  Module that monitors the PIDs of consumers. When a consumers
  is created it will start monitoring the PID and store it in the state.
  When a consumer is shutdown it will push the consumers status to cogynt-otp
  and remove the PID from the state.
  """
  use GenServer
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.ConsumerStateManager

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid, id, topic) do
    GenServer.cast(__MODULE__, {:monitor, pid, id, topic})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid, event_definition_id, topic}, state) do
    Process.monitor(pid)

    new_state = Map.put(state, pid, %{event_definition_id: event_definition_id, topic: topic})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    case reason do
      :shutdown ->
        new_state = Map.delete(state, pid)
        {:noreply, new_state}

      :normal ->
        new_state = Map.delete(state, pid)
        {:noreply, new_state}

      _ ->
        %{event_definition_id: event_definition_id, topic: topic} = Map.get(state, pid)

        {:ok, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition_id)

        case ConsumerStateManager.finished_processing?(event_definition_id) do
          {:ok, true} ->
            check_consumer_state(
              event_definition_id,
              topic,
              consumer_state.status,
              ConsumerStatusTypeEnum.status()[:paused_and_finished]
            )

          {:ok, false} ->
            check_consumer_state(
              event_definition_id,
              topic,
              consumer_state.status,
              ConsumerStatusTypeEnum.status()[:paused_and_processing]
            )
        end

        new_state = Map.delete(state, pid)
        {:noreply, new_state}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp check_consumer_state(id, topic, status, new_status) do
    cond do
      status == new_status or
        status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
          status == ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
        Redis.publish_async("consumer_state_subscription", %{
          id: id,
          topic: topic,
          status: new_status
        })

      true ->
        ConsumerStateManager.upsert_consumer_state(id,
          topic: topic,
          status: new_status,
          module: __MODULE__
        )
    end
  end
end
