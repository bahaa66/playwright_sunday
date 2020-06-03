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
      case EventsContext.initalize_consumers_with_active_event_definitions() do
        {:ok, _} ->
          CogyntLogger.info("#{__MODULE__}", "Consumers Initialized")

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to initialize consumers. Error: #{inspect(error)}"
          )
      end
    else
      {:error, error} ->
        CogyntLogger.error("#{__MODULE__}", "App not started. #{inspect(error)}")
    end
  end
end
