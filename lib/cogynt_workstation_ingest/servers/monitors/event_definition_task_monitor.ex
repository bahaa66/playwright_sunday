defmodule CogyntWorkstationIngest.Servers.EventDefinitionTaskMonitor do
  @moduledoc """
    Module that monitors the the status of EventDefinition Tasks.
    Will publish via pub/sub when task is completed and store
    status of task in Redis.
  """

  use GenServer

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid, event_definition_ids) do
    GenServer.cast(__MODULE__, {:monitor, pid, event_definition_ids})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid, event_definition_ids}, state) do
    Process.monitor(pid)
    new_state = Map.put(state, pid, event_definition_ids)

    case is_list(event_definition_ids) do
      true ->
        Enum.each(event_definition_ids, fn event_definition_id ->
          Redis.hash_set_async("ts", event_definition_id, %{
            status: "running",
            hard_delete: false
          })
        end)

        # TODO: implement handler for this on cogynt-otp
        Redis.publish_async("event_definitions_subscription", %{
          event_definition_ids: event_definition_ids,
          deleting: true
        })

      false ->
        Redis.hash_set_async("ts", event_definition_ids, %{
          status: "running",
          hard_delete: false
        })

        # TODO: implement handler for this on cogynt-otp
        Redis.publish_async("event_definitions_subscription", %{
          event_definition_ids: [event_definition_ids],
          deleting: true
        })
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # TODO implement retry for backfill/update task if reason anything other than :normal or :shutdown

    val = Map.get(state, pid)

    if is_list(val) do
      Enum.each(val, fn event_definition_id ->
        Redis.hash_delete("ts", event_definition_id)
      end)

      # TODO: implement handler for this on cogynt-otp
      Redis.publish_async("event_definitions_subscription", %{
        event_definition_ids: val,
        deleting: false
      })

      {:noreply, Map.delete(state, pid)}
    else
      Redis.hash_delete("ts", val)

      # TODO: implement handler for this on cogynt-otp
      Redis.publish_async("event_definitions_subscription", %{
        event_definition_ids: [val],
        deleting: false
      })
    end

    {:noreply, Map.delete(state, pid)}
  end

  @doc false
  def event_definition_task_running?(event_definition_id) do
    case Redis.hash_get("ts", event_definition_id) do
      {:ok, nil} ->
        false

      {:error, _} ->
        false

      _ ->
        true
    end
  end
end
