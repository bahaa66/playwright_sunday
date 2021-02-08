defmodule CogyntWorkstationIngest.Servers.Monitors.DrilldownSinkConnectorMonitor do
  @doc """
  Module that periodically checks if SinkConnector is alive and running, else restarts the task.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Drilldown.DrilldownSinkConnector

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def update_paused_state(paused) do
    GenServer.cast(__MODULE__, {:update_paused_state, paused})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    schedule_status_check()
    {:ok, %{paused: false}}
  end

  @impl true
  def handle_cast({:update_paused_state, paused}, state) do
    new_state = Map.put(state, :paused, paused)
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:check_drilldown_connector_status, %{paused: false} = state) do
    ts_connector_name =
      DrilldownSinkConnector.fetch_drilldown_connector_cgid(Config.ts_connector_name())

    tse_connector_name =
      DrilldownSinkConnector.fetch_drilldown_connector_cgid(Config.tse_connector_name())

    with {:ok, %{connector: %{state: _state}, tasks: [%{id: ts_id, state: ts_state}]}} <-
           DrilldownSinkConnector.connector_status?(ts_connector_name),
         {:ok, %{connector: %{state: _state}, tasks: [%{id: tse_id, state: tse_state}]}} <-
           DrilldownSinkConnector.connector_status?(tse_connector_name) do
      cond do
        ts_state != "FAILED" and tse_state != "FAILED" ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "DrilldownSinkConnector task is #{ts_state}... do not restart task"
          )

          schedule_status_check()
          {:noreply, state}

        ts_state == "FAILED" and tse_state == "FAILED" ->
          DrilldownSinkConnector.restart_task(ts_connector_name, ts_id)
          DrilldownSinkConnector.restart_task(tse_connector_name, tse_id)
          schedule_status_check()
          {:noreply, state}

        ts_state == "FAILED" ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "#{ts_connector_name} task is not running... state: #{ts_state} so, restarting task"
          )

          DrilldownSinkConnector.restart_task(ts_connector_name, ts_id)
          schedule_status_check()
          {:noreply, state}

        tse_state == "FAILED" ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "#{tse_connector_name} task is not running... state: #{tse_state} so, restarting task"
          )

          DrilldownSinkConnector.restart_task(tse_connector_name, tse_id)
          schedule_status_check()
          {:noreply, state}
      end
    else
      {:error, :not_found} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "DrilldownSinkConnector has error code 404 with message: Not Found"
        )

        CogyntLogger.error(
          "#{__MODULE__}",
          "DrilldownSinkConnector does not exist so creating it."
        )

        DrilldownSinkConnector.create_or_update()
        schedule_status_check()
        {:noreply, state}

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "DrilldownSinkConnector has error in connection... #{inspect(reason)}"
        )

        schedule_status_check()
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:check_drilldown_connector_status, %{paused: true} = state) do
    schedule_status_check()
    {:noreply, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  def schedule_status_check() do
    Process.send_after(
      __MODULE__,
      :check_drilldown_connector_status,
      Config.connector_restart_time_delay()
    )
  end
end
