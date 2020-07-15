defmodule CogyntWorkstationIngest.Servers.Startup do
  @moduledoc """
  Genserver Module that is used for tasks that need to run upon Application startup
  """
  use GenServer
  alias CogyntWorkstationIngest.Events.EventsContext

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_info(:initialize_consumers, state) do
    CogyntLogger.info("#{__MODULE__}", "Initializing Consumers")
    initialize_consumers()
    {:noreply, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp initialize_consumers() do
    with :ok <- Application.ensure_started(:phoenix),
         :ok <- Application.ensure_started(:postgrex) do
      EventsContext.start_consumers_for_active_ed()
      EventsContext.init_consumer_state_for_inactive_ed()
      CogyntLogger.info("#{__MODULE__}", "Consumers Initialized")
    else
      {:error, error} ->
        CogyntLogger.error("#{__MODULE__}", "App not started. #{inspect(error, pretty: true)}")
    end
  end
end
