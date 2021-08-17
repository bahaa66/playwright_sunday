defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor do
  @moduledoc """
  A Druid Ingestion Supervisor that is started by a DynamicSupervisor. This keeps track
  of the status of the Druid Spuervisor and can execute actions against it
  """
  use GenServer, restart: :transient

  @status_check_interval 30_000
  @dss_key_expire 300_000
  @detailed_state_errors [
    "UNHEALTHY_SUPERVISOR",
    "UNHEALTHY_TASKS",
    "UNABLE_TO_CONNECT_TO_STREAM",
    "LOST_CONTACT_WITH_STREAM"
  ]

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(%{supervisor_id: supervisor_id} = args) do
    GenServer.start_link(__MODULE__, args, name: {:via, Registry, {DruidRegistry, supervisor_id}})
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

  def create_or_update_supervisor(pid, opts) do
    GenServer.cast(pid, {:create_or_update_supervisor, opts})
  end

  def suspend_supervisor(pid) do
    GenServer.call(pid, :suspend_supervisor)
  end

  def resume_supervisor(pid) do
    GenServer.call(pid, :resume_supervisor)
  end

  def terminate_and_shutdown(pid) do
    GenServer.call(pid, :terminate_and_shutdown)
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
        state = %{id: supervisor_id, supervisor_status: %{state: "LOADING"}}
        Redis.hash_set_async("dss", supervisor_id, state)
        Redis.key_pexpire("dss", @dss_key_expire)

        # TODO Return a sync error if fails to create supervisor upon startup
        {:ok, state, {:continue, {:create_or_update_supervisor, args}}}

      {:ok, false} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "The Druid server is unhealthy."
        )

        {:stop, :unhealthy_druid_server}

      {:error, :internal_server_error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to verify the health of the Druid server. internal_server_error}"
        )

        {:stop, :druid_server_connection_error}
    end
  end

  @impl true
  def handle_call(:supervisor_status, _from, %{id: id} = state) do
    case Redis.hash_get("dss", id) do
      {:ok, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "DruidSupervisorMonitor State not found. Resetting Supervisor and State"
        )

        {:reply, "NOT FOUND", state, {:continue, :reset_and_get_supervisor}}

      {:ok, %{supervisor_status: status} = state} ->
        {:reply, status, state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:healthy?, _from, %{id: id} = state) do
    case Redis.hash_get("dss", id) do
      {:ok, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "DruidSupervisorMonitor State not found. Resetting Supervisor and State"
        )

        {:reply, "NOT FOUND", state, {:continue, :reset_and_get_supervisor}}

      {:ok, %{supervisor_status: status} = state} ->
        {:reply, Map.get(status, :healthy, false), state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:state, _from, %{id: id} = state) do
    case Redis.hash_get("dss", id) do
      {:ok, nil} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "DruidSupervisorMonitor State not found. Resetting Supervisor and State"
        )

        {:reply, "NOT FOUND", state, {:continue, :reset_and_get_supervisor}}

      {:ok, %{supervisor_status: status} = state} ->
        {:reply, Map.get(status, :state, "UNKNOWN"), state}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_cast({:create_or_update_supervisor, opts}, state) do
    {:noreply, state, {:continue, {:create_or_update_supervisor, opts}}}
  end

  @impl true
  def handle_call(:delete_data_and_reset_supervisor, _from, %{id: id} = state) do
    Druid.delete_datasource(id)
    |> case do
      {:ok, response} ->
        state = %{state | supervisor_status: %{state: "DELETING"}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)
        {:reply, response, state, {:continue, :reset_and_get_supervisor}}

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
        state = %{state | supervisor_status: %{state: "SUSPENDED"}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)
        {:reply, response, state}

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
        state = %{state | supervisor_status: %{state: "RUNNING"}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)
        {:reply, response, state}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to resume Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_call(:terminate_and_shutdown, _from, %{id: id} = state) do
    Druid.terminate_supervisor(id)
    |> case do
      {:ok, response} ->
        state = %{state | supervisor_status: %{state: "TERMINATED"}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)
        {:reply, response, state, {:continue, :shutdown_server}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to terminate Druid supervisor do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl true
  def handle_continue({:create_or_update_supervisor, args}, %{id: id} = state) do
    brokers = Map.get(args, :brokers)
    schema = Map.get(args, :schema, :json)

    supervisor_specs =
      Map.take(args, [
        :dimensions_spec,
        :supervisor_id,
        :io_config,
        :parse_spec,
        :flatten_spec,
        :granularity_spec,
        :timestamp_spec,
        :schema_registry_url,
        :topic
      ])
      |> Keyword.new()

    supervisor_spec = Druid.Utils.build_kafka_supervisor(id, brokers, schema, supervisor_specs)

    with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      Process.send_after(__MODULE__, :get_status, @status_check_interval)

      state = %{state | supervisor_status: payload}
      Redis.hash_set_async("dss", id, state)
      Redis.key_pexpire("dss", @dss_key_expire)
      {:noreply, state}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to create/fetch Druid supervisor information for #{id}: #{inspect(error)}"
        )

        state = %{state | supervisor_status: {:error, :internal_server_error}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)

        {:noreply, state}
    end
  end

  @impl true
  def handle_continue(:reset_and_get_supervisor, %{id: id} = state) do
    with {:ok, %{"id" => id}} <- Druid.reset_supervisor(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      state = %{state | supervisor_status: payload}
      Redis.hash_set_async("dss", id, state)
      Redis.key_pexpire("dss", @dss_key_expire)
      {:noreply, state}
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
  def handle_continue(:shutdown_server, %{id: id} = _state) do
    Redis.hash_delete("dss", id)
    Process.exit(self(), :normal)
  end

  @impl true
  def handle_info(:get_status, %{id: id} = state) do
    Druid.get_supervisor_status(id)
    |> case do
      {:ok, %{"payload" => %{"detailedState" => detailed_state} = payload}} ->
        if Enum.member?(@detailed_state_errors, detailed_state) do
          CogyntLogger.warn(
            "#{__MODULE__}",
            "DruidSupervisor: #{id} in Error State: #{detailed_state}. Resetting Supervisor"
          )

          {:noreply, state, {:continue, :reset_and_get_supervisor}}
        else
          Process.send_after(__MODULE__, :get_status, @status_check_interval)
          state = %{state | supervisor_status: payload}
          Redis.hash_set_async("dss", id, state)
          Redis.key_pexpire("dss", @dss_key_expire)
          {:noreply, state}
        end

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to get Druid supervisor status for #{id}: #{inspect(error)}"
        )

        state = %{state | supervisor_status: {:error, :internal_server_error}}
        Redis.hash_set_async("dss", id, state)
        Redis.key_pexpire("dss", @dss_key_expire)

        {:noreply, state}
    end
  end
end
