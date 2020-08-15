defmodule CogyntWorkstationIngest.Servers.Caches.DeleteEventDefinitionDataCache do
  @moduledoc """
  Keeps track of the EventDefinitionId status when performing a delete_task.
  Need to store this data because a delete task cannot be completed until a pipeline
  is finished processing all data for a given EventDefinitionId
  """
  use GenServer
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def get_status(event_definition_id) do
    IO.puts("GET STATUS")
    GenServer.call(__MODULE__, {:get_status, event_definition_id})
  end

  def upsert_status(event_definition_id, args) do
    IO.inspect(args, label: "UPDATING STATUS")
    GenServer.cast(__MODULE__, {:upsert_status, event_definition_id, args})
  end

  def remove_status(event_definition_id) do
    IO.puts("REMOVING STATUS")
    GenServer.cast(__MODULE__, {:remove_status, event_definition_id})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_status, event_definition_id}, _from, state) do
    {:reply, Map.get(state, event_definition_id, nil), state}
  end

  @impl true
  def handle_cast({:upsert_status, event_definition_id, args}, state) do
    new_status = Keyword.get(args, :status)
    hard_delete = Keyword.get(args, :hard_delete, false)

    case Map.get(state, event_definition_id, nil) do
      nil ->
        {:noreply,
         Map.put(
           state,
           event_definition_id,
           Map.new([{:status, new_status}, {:hard_delete, hard_delete}])
         )}

      %{status: status, hard_delete: delete} = ed ->
        cond do
          status == :waiting and new_status == :ready ->
            Process.sleep(1000)

            TaskSupervisor.start_child(%{
              delete_event_definitions_and_topics: %{
                event_definition_ids: [event_definition_id],
                hard_delete: delete,
                delete_topics: false
              }
            })

            {:noreply, Map.delete(state, event_definition_id)}

          true ->
            new_ed_state = Map.put(ed, :status, new_status)
            {:noreply, Map.put(state, event_definition_id, new_ed_state)}
        end
    end
  end

  @impl true
  def handle_cast({:remove_status, event_definition_id}, state) do
    {:noreply, Map.delete(state, event_definition_id)}
  end
end
