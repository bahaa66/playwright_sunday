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

  @doc """
  Fetch the current Druid supervisor status
  """
  def supervisor_status(pid) do
    GenServer.call(pid, :supervisor_status)
  end

  @doc """
  Checks if the current Druid supervisor is Healthy
  """
  def healthy?(pid) do
    GenServer.call(pid, :healthy?)
  end

  @doc """
  Fetch the current Druid supervisor state
  """
  def supervisor_state(pid) do
    GenServer.call(pid, :supervisor_state)
  end

  @doc """
  Will create a Druid supervisor with the opts passed or update
  one if one already exists
  """
  def create_or_update_supervisor(pid, opts) do
    GenServer.cast(pid, {:create_or_update_supervisor, opts})
  end

  @doc """
  Suspends the current Druid supervisor
  """
  def suspend_supervisor(pid) do
    GenServer.call(pid, :suspend_supervisor, 10000)
  end

  @doc """
  Resumes the current Druid supervisor
  """
  def resume_supervisor(pid) do
    GenServer.call(pid, :resume_supervisor, 10000)
  end

  @doc """
  Drops the segmants for the Druid datasource by marking
  them as unused. Then resets the Druid supervisor to re ingest
  the data
  """
  def delete_data_and_reset_supervisor(pid) do
    GenServer.call(pid, :delete_data_and_reset_supervisor)
  end

  @doc """
  Drops the segmants for the Druid datasource by marking
  them as unused. Then terminates the Druid supervisor and
  shutsdown the Druid monitor
  """
  def delete_data_and_terminate_supervisor(pid) do
    GenServer.call(pid, :delete_data_and_terminate_supervisor)
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
              "Druid supervisor not found. Creating one now: #{inspect(error)}"
            )

            handle_supervisor(supervisor_id, args)

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to create/fetch Druid supervisor information for #{supervisor_id}: #{inspect(error)}"
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
          "Unable to Create/Fetch Druid supervisor information for #{id}: #{inspect(error)}"
        )

        {:noreply, state, {:continue, :shutdown_server}}
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
          "Unable to Suspend Druid datasource: #{id} do to error: #{inspect(error)}"
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
          "Unable to Resume Druid datasource: #{id} do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call(:delete_data_and_reset_supervisor, _from, %{id: id} = state) do
    with {:ok, delete_response} <- Druid.datasource_segmants_mark_unused(id),
         {:ok, datasources} <- Druid.list_datasources_with_used_segmants(),
         {:ok, _response} <- wait_while_dropping_segmants(id, datasources) do
      {:reply, delete_response, state, {:continue, :reset_and_get_supervisor}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to Drop Druid segments for Datasource: #{id} do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call(:delete_data_and_terminate_supervisor, _from, %{id: id} = state) do
    with {:ok, delete_response} <- Druid.datasource_segmants_mark_unused(id),
         {:ok, datasources} <- Druid.list_datasources_with_used_segmants(),
         {:ok, _response} <- wait_while_dropping_segmants(id, datasources) do
      IO.inspect(datasources, label: "List of Datasources")
      {:reply, delete_response, state, {:continue, :terminate_and_shutdown}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to Drop Druid segments for Datasource: #{id} do to error: #{inspect(error)}"
        )

        {:reply, {:error, error}, state}
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
          "Unable to Reset/Fetch supervisor Druid information for #{id}: #{inspect(error)}"
        )

        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:terminate_and_shutdown, %{id: id} = state) do
    Druid.terminate_supervisor(id)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Shutting down Druid Supervisor Monitor for Datasource: #{id}"
    )

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
      {:error, %{code: 404} = _error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Supervisor #{id} no longer exists in Druid. Shutting down genserver."
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

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

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
          "Unable to Create/Fetch Druid supervisor information for #{supervisor_id}: #{inspect(error)}"
        )

        {:stop, :failed_to_create_druid_supervisor}
    end
  end

  defp wait_while_pending(datasource_name, status, counter \\ 0) do
    if counter >= 10 do
      CogyntLogger.warn(
        "#{__MODULE__}",
        "wait_while_pending/3 exceeded the number of retry attempts. Druid Supervisor is still in PENDING state for Datasource: #{datasource_name}"
      )

      {:ok, status}
    else
      case SupervisorStatus.is_pending?(status) do
        true ->
          # Give Druid some time to execute whatever it is PENDING for
          Process.sleep(800)
          {:ok, %{"payload" => payload}} = Druid.get_supervisor_status(datasource_name)
          %SupervisorStatus{} = status = SupervisorStatus.new(payload)
          wait_while_pending(datasource_name, status, counter + 1)

        false ->
          {:ok, status}
      end
    end
  end

  defp wait_while_dropping_segmants(datasource_name, datasources, counter \\ 0) do
    if counter >= 10 do
      CogyntLogger.warn(
        "#{__MODULE__}",
        "wait_while_dropping_segmants/3 exceeded the number of retry attempts (10). Druid still dropping segmants for Datasource: #{datasource_name}"
      )

      {:ok, :exceeded_retry_count}
    else
      case Enum.member?(datasources, datasource_name) do
        true ->
          # Give Druid some time to drop the segments for the datasource
          Process.sleep(800)
          {:ok, datasources} = Druid.list_datasources_with_used_segmants()
          wait_while_dropping_segmants(datasource_name, datasources, counter + 1)

        false ->
          {:ok, :segments_dropped}
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
