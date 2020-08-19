defmodule CogyntWorkstationIngest.Servers.Caches.DrilldownCache do
  @moduledoc """
  Genserver Module that keeps Drilldown template solution data stored
  in the state
  """
  use GenServer
  alias CogyntWorkstationIngest.Config

  # TODO needs to have state moved to PSQL or Redis

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def get(id) do
    GenServer.call(__MODULE__, {:get_solution, id})
  end

  def list() do
    GenServer.call(__MODULE__, :list)
  end

  def put(data) do
    GenServer.cast(__MODULE__, {:put_data, data})
  end

  def reset_state() do
    GenServer.cast(__MODULE__, :reset_state)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_solution, id}, _from, state) do
    {:reply, {:ok, Map.get(state, id)}, state}
  end

  @impl true
  def handle_call(:list, _from, state) do
    {:reply, {:ok, state}, state}
  end

  @impl true
  def handle_cast({:put_data, %{sol_id: id, sol: sol, evnt: evnt} = data}, state) do
    # IO.inspect(data, label: "@@@@ Received event")

    sol =
      (state[id] || %{"events" => %{}, "outcomes" => []})
      |> Map.merge(sol)

    state =
      cond do
        Map.has_key?(data, :event) and not Map.has_key?(data.event, "aid") ->
          sol =
            sol
            |> Map.put("outcomes", [evnt | sol["outcomes"]])

          Map.put(state, sol["id"], sol)

        sol["id"] == evnt["published_by"] ->
          # event is input and published by same instance
          state

        Map.has_key?(data, :event) and Map.has_key?(data.event, "aid") ->
          key = evnt["id"] <> "!" <> evnt["assertion_id"]
          replace = sol["events"][key]

          if replace != nil do
            # IO.inspect(evnt, label: "@@@@ Received event")
            # IO.inspect(replace, label: "@@@@ Replacing")
          end

          sol =
            sol
            |> Map.put("events", Map.put(sol["events"], key, evnt))

          Map.put(state, sol["id"], sol)

        true ->
          # should not reach here
          state
      end

    {:noreply, state}
  end

  @impl true
  def handle_cast({:put_data, %{sol_id: id, sol: sol}}, state) do
    # IO.inspect("@@@@ Received solution #{id}")

    sol =
      (state[id] || %{"events" => %{}, "outcomes" => []})
      |> Map.merge(sol)

    state = Map.put(state, id, sol)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:reset_state, _state) do
    {:noreply, %{}}
  end
end
