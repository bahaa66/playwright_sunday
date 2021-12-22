defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor do
  @moduledoc """
  A Druid Ingestion Supervisor that is started by a DynamicSupervisor. This keeps track
  of the status of the Druid Spuervisor and can execute actions against it
  """
  use GenServer
  alias __MODULE__.SupervisorStatus

  @status_check_interval 90_000
  @starting_status_interval 1_000

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    druid_spec = Keyword.get(opts, :druid_spec, %{})
    force_update = Keyword.get(opts, :force_update, false)
    GenServer.start_link(__MODULE__, [druid_spec, force_update], name: name)
  end

  def supervisor_status(pid) do
    GenServer.call(pid, :supervisor_status)
  end

  def healthy?(pid) do
    GenServer.call(pid, :healthy?)
  end

  def supervisor_state(pid) do
    GenServer.call(pid, :supervisor_state)
  end

  def create_or_update_supervisor(pid, opts) do
    GenServer.cast(pid, {:create_or_update_supervisor, opts})
  end

  def suspend_supervisor(pid) do
    GenServer.call(pid, :suspend_supervisor, 10000)
  end

  def resume_supervisor(pid) do
    GenServer.call(pid, :resume_supervisor, 10000)
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
  @impl GenServer
  def init([%{supervisor_id: supervisor_id} = args, true]) do
    handle_supervisor(supervisor_id, args)
  end

  @impl GenServer
  def init([%{supervisor_id: supervisor_id} = args, false]) do
    Druid.status_health()
    |> case do
      {:ok, true} ->
        with {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(supervisor_id),
             %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
          schedule(status)
          {:ok, %{id: supervisor_id, supervisor_status: status}}
        else
          {:error, %{code: 404} = error} ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Druid supervisor not found. Creating one now: #{
                inspect(error)
              }"
            )

            handle_supervisor(supervisor_id, args)

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to create/fetch Druid supervisor information for #{supervisor_id}: #{
                inspect(error)
              }"
            )

            {:stop, :failed_to_create_druid_supervisor}
        end

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

  defp handle_supervisor(supervisor_id, args) do
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

    supervisor_spec =
      Druid.Utils.build_kafka_supervisor(
        supervisor_id,
        brokers,
        schema,
        supervisor_specs
      )

    with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
      schedule(status)
      {:ok, %{id: id, supervisor_status: status}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to create/fetch Druid supervisor information for #{supervisor_id}: #{
            inspect(error)
          }"
        )

        {:stop, :failed_to_create_druid_supervisor}
    end
  end

  @impl GenServer
  def handle_call(:supervisor_status, _from, %{supervisor_status: status} = state) do
    {:reply, status, state}
  end

  @impl GenServer
  def handle_call(:healthy?, _from, %{supervisor_status: status} = state) do
    {:reply, Map.get(status, :healthy, false), state}
  end

  @impl GenServer
  def handle_call(
        :supervisor_state,
        _from,
        %{supervisor_status: %SupervisorStatus{state: sup_state}} = state
      ) do
    {:reply, sup_state, state}
  end

  @impl GenServer
  def handle_call(:delete_data_and_reset_supervisor, _from, %{id: id} = state) do
    with {:ok, delete_response} <- Druid.delete_datasource(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
      {:reply, delete_response, %{state | supervisor_status: status},
       {:continue, :reset_and_get_supervisor}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to delete Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call(:suspend_supervisor, _from, %{id: id} = state) do
    with {:ok, suspend_response} <- Druid.suspend_supervisor(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload),
         {:ok, status} <- wait_while_pending(id, status) do
      {:reply, suspend_response, %{state | supervisor_status: status}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to suspend Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call(:resume_supervisor, _from, %{id: id} = state) do
    with {:ok, resume_response} <- Druid.resume_supervisor(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload),
         {:ok, status} <- wait_while_pending(id, status) do
      {:reply, resume_response, %{state | supervisor_status: status}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to resume Druid datasource do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call(:terminate_and_shutdown, _from, %{id: id} = state) do
    Druid.terminate_supervisor(id)
    |> case do
      {:ok, response} ->
        {:reply, response, state, {:continue, :shutdown_server}}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to terminate Druid supervisor do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_cast({:create_or_update_supervisor, args}, %{id: id} = state) do
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
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
      {:noreply, %{state | supervisor_status: status}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to create/fetch Druid supervisor information for #{id}: #{inspect(error)}"
        )

        {:noreply, state, {:continue, :shutdown_server}}
    end
  end

  @impl GenServer
  def handle_continue(:reset_and_get_supervisor, %{id: id} = state) do
    with {:ok, %{"id" => id}} <- Druid.reset_supervisor(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
      {:noreply, %{state | supervisor_status: status}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to reset/fetch supervisor Druid information for #{id}: #{inspect(error)}"
        )

        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:shutdown_server, %{id: id} = state) do
    CogyntLogger.info("#{__MODULE__}", "Shutting down Druid Supervisor Monitor for ID: #{id}")
    {:stop, :normal, state}
  end

  @impl GenServer
  def handle_info(:update_status, %{id: id} = state) do
    with {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id),
         %SupervisorStatus{} = status <- SupervisorStatus.new(payload) do
      if SupervisorStatus.requires_reset?(status) do
        CogyntLogger.warn(
          "#{__MODULE__}",
          "DruidSupervisor: #{id} in Error State: #{inspect(status)}. Resetting Supervisor"
        )

        schedule(status)
        {:noreply, state, {:continue, :reset_and_get_supervisor}}
      else
        schedule(status)
        {:noreply, %{state | supervisor_status: status}}
      end
    else
      {:error, %{code: 400, error: "Cannot find any supervisor with id:" <> name}} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Supervisor #{name} no longer exists in druid. Shutting down genserver."
        )

        {:stop, :normal, state}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to get Druid supervisor status for #{id}: #{inspect(error)}"
        )

        schedule("ERROR")
        {:noreply, state}
    end
  end

  defp wait_while_pending(id, status, counter \\ 0) do
    if counter >= 10 do
      CogyntLogger.warn(
        "#{__MODULE__}",
        "wait_while_pending/3 exceeded the number of retry attempts. Druid Supervisor is still in PENDING state for ID: #{
          id
        }"
      )

      {:ok, status}
    else
      case SupervisorStatus.is_pending?(status) do
        true ->
          # Give Druid some time to execute whatever it is PENDING for
          Process.sleep(800)
          {:ok, %{"payload" => payload}} = Druid.get_supervisor_status(id)
          %SupervisorStatus{} = status = SupervisorStatus.new(payload)
          wait_while_pending(id, status, counter + 1)

        false ->
          {:ok, status}
      end
    end
  end

  defp schedule(%SupervisorStatus{state: state}) when state in ["PENDING", "CREATING_TASKS"] do
    Process.send_after(self(), :update_status, @starting_status_interval)
  end

  defp schedule(_) do
    Process.send_after(self(), :update_status, @status_check_interval)
  end
end
