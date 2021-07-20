defmodule CogyntWorkstationIngest.Servers.Druid.DynamicSupervisorMonitor do
  @moduledoc """
  A Druid Ingestion Supervisor that is started by a DynamicSupervisor. This keeps track
  of the status of the Druid Spuervisor and can execute actions against it
  """
  use GenServer

  @status_check_interval 10_000

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(%{name: name} = args) do
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def supervisor_status(pid) do
    GenServer.call(pid, :supervisor_status)
  end

  def healthy?(pid) do
    GenServer.call(pid, :healthy?)
  end

  def state(pid) do
    GenServer.call(pid, :state)
  end

  def suspend_supervisor(pid) do
    GenServer.call(pid, :suspend_supervisor)
  end

  def resume_supervisor(pid) do
    GenServer.call(pid, :resume_supervisor)
  end

  def delete_data_and_reset_supervisor(pid) do
    GenServer.call(pid, :delete_data_and_reset_supervisor)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(%{supervisor_id: supervisor_id} = args) do
    Druid.status_health()
    |> case do
      {:ok, true} ->
        {:ok, %{id: supervisor_id, supervisor_status: %{"state" => "LOADING"}},
         {:continue, {:create_or_update_supervisor, args}}}

      {:ok, false} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "The Druid server is unhealthy."
        )

        {:stop, :unhealthy_druid_server}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to verify the health of the Druid server: #{inspect(error)}"
        )

        {:stop, :druid_server_connection_error}
    end
  end

  @impl true
  def handle_call(:supervisor_status, _from, %{supervisor_status: status} = state) do
    {:reply, status, state}
  end

  @impl true
  def handle_call(:healthy?, _from, %{supervisor_status: status} = state) do
    {:reply, Map.get(status, "healthy", false), state}
  end

  @impl true
  def handle_call(:state, _from, %{supervisor_status: status} = state) do
    {:reply, Map.get(status, "state", "UNKNOWN"), state}
  end

  @impl true
  def handle_call(:delete_data_and_reset_supervisor, _from, %{id: id} = state) do
    Druid.delete_datasource(id)
    |> case do
      {:ok, response} ->
        {:reply, response, %{state | supervisor_status: %{"state" => "DELETING"}},
         {:continue, :reset_and_get_supervisor}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to delete Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:suspend_supervisor, _from, %{id: id} = state) do
    Druid.suspend_supervisor(id)
    |> case do
      {:ok, response} ->
        IO.inspect(response, label: "SUSPEND RESPONSE", pretty: true)
        {:reply, response, %{state | supervisor_status: %{"state" => "SUSPENDED"}}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to suspend Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:resume_supervisor, _from, %{id: id} = state) do
    Druid.resume_supervisor(id)
    |> case do
      {:ok, response} ->
        IO.inspect(response, label: "RESUME RESPONSE", pretty: true)
        {:reply, response, %{state | supervisor_status: %{"state" => "RUNNING"}}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to resume Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_continue({:create_or_update_supervisor, args}, %{id: id} = state) do
    dimensions_spec = Map.get(args, :dimensions_spec, %{dimensions: []})
    brokers = Map.get(args, :brokers)
    io_config = Map.get(args, :io_config)
    granularity_spec = Map.get(args, :granularity_spec)
    timestamp_spec = Map.get(args, :timestamp_spec)

    supervisor_spec =
      Druid.Utils.build_kafka_supervisor(id, brokers,
        dimensions_spec: dimensions_spec,
        io_config: io_config,
        granularity_spec: granularity_spec,
        timestamp_spec: timestamp_spec
      )

    with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      Process.send_after(__MODULE__, :get_status, @status_check_interval)
      {:noreply, %{state | supervisor_status: payload}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to create/fetch Druid supervisor information for #{id}: #{inspect(error)}"
        )

        {:noreply, %{state | supervisor_status: {:error, error}}}
    end
  end

  @impl true
  def handle_continue(:reset_and_get_supervisor, %{id: id} = state) do
    with {:ok, %{"id" => id}} <- Druid.reset_supervisor(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      {:noreply, %{state | supervisor_status: payload}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to reset/fetch supervisor Druid information for #{id}: #{inspect(error)}"
        )

        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:get_status, %{id: id} = state) do
    Druid.get_supervisor_status(id)
    |> case do
      {:ok, %{"payload" => %{"detailedState" => "LOST_CONTACT_WITH_STREAM"} = _p}} ->
        {:noreply, state, {:continue, :reset_and_get_supervisor}}

      {:ok, %{"payload" => payload} = _s} ->
        Process.send_after(__MODULE__, :get_status, @status_check_interval)
        {:noreply, %{state | supervisor_status: payload}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to get Druid supervisor status for #{id}: #{inspect(error)}"
        )

        {:noreply, %{state | supervisor_status: {:error, error}}}
    end
  end
end
