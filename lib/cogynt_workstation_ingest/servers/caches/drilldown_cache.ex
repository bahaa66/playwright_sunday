defmodule CogyntWorkstationIngest.Servers.Caches.DrilldownCache do
  @moduledoc """
  Genserver Module that keeps Drilldown template solution data stored
  in the state
  """
  use GenServer
  require Logger
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

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

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    Process.send_after(__MODULE__, :tick, time_delay())
    {:ok, %{}}
  end

  @impl true
  def handle_info(:tick, state) do
    ConsumerGroupSupervisor.start_child()
    {:noreply, state}
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
        not Map.has_key?(data, "aid") ->
          sol =
            sol
            |> Map.put("outcomes", [evnt | sol["outcomes"]])

          Map.put(state, sol["id"], sol)

        sol["id"] == evnt["published_by"] ->
          # event is input and published by same instance
          state

        Map.has_key?(data, "aid") ->
          key = evnt["id"] <> "!" <> evnt["assertion_id"]
          replace = sol["events"][key]

          if replace != nil do
            IO.inspect(evnt, label: "@@@@ Received event")
            IO.inspect(replace, label: "@@@@ Replacing")
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

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp time_delay(), do: config()[:time_delay]
end
