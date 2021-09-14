defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor do
  @moduledoc """
  A Druid Ingestion Supervisor that is started by a DynamicSupervisor. This keeps track
  of the status of the Druid Spuervisor and can execute actions against it
  """
  use GenServer

  @detailed_state_errors [
    "UNHEALTHY_SUPERVISOR",
    "UNHEALTHY_TASKS",
    "UNABLE_TO_CONNECT_TO_STREAM",
    "LOST_CONTACT_WITH_STREAM"
  ]
  @status_check_interval 90_000
  @starting_status_interval 1_000

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    druid_spec = Keyword.get(opts, :druid_spec, %{})
    GenServer.start_link(__MODULE__, druid_spec, name: name)
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

  # # ------------------------ #
  # # --- server callbacks --- #
  # # ------------------------ #
  @impl GenServer
  def init(%{supervisor_id: supervisor_id} = args) do
    Druid.status_health()
    |> case do
      {:ok, true} ->
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
          Druid.Utils.build_kafka_supervisor(supervisor_id, brokers, schema, supervisor_specs)

        with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
             {:ok, %{"payload" => %{"detailedState" => status} = payload}} <-
               Druid.get_supervisor_status(id) do
          schedule(status)
          {:ok, %{id: supervisor_id, supervisor_status: payload}}
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
  def handle_call(:supervisor_status, _from, %{supervisor_status: %{"state" => status}} = state) do
    {:reply, status, state}
  end

  @impl GenServer
  def handle_call(:healthy?, _from, %{supervisor_status: status} = state) do
    {:reply, Map.get(status, :healthy, false), state}
  end

  @impl GenServer
  def handle_call(:state, _from, %{supervisor_status: status} = state) do
    {:reply, status, state}
  end

  @impl GenServer
  def handle_call(:delete_data_and_reset_supervisor, _from, %{id: id} = state) do
    with {:ok, delete_response} <- Druid.delete_datasource(id),
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      {:reply, delete_response, %{state | supervisor_status: payload},
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
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      {:reply, suspend_response, %{state | supervisor_status: payload}}
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
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      {:reply, resume_response, %{state | supervisor_status: payload}}
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
         {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
      {:noreply, %{state | supervisor_status: payload}}
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
         {:ok, %{"payload" => %{"detailedState" => status}} = payload} <-
           Druid.get_supervisor_status(id) do
      schedule(status)
      {:noreply, %{state | supervisor_status: payload}}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to reset/fetch supervisor Druid information for #{id}: #{inspect(error)}"
        )

        schedule("ERROR")
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:shutdown_server, %{id: id} = _state) do
    CogyntLogger.info("#{__MODULE__}", "Shutting down Druid Supervisor Monitor for ID: #{id}")
    Process.exit(self(), :normal)
  end

  @impl GenServer
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
          schedule(detailed_state)
          {:noreply, %{state | supervisor_status: payload}}
        end

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to get Druid supervisor status for #{id}: #{inspect(error)}"
        )

        schedule("ERROR")
        {:noreply, state}
    end
  end

  defp schedule(status) when status in ["PENDING", "CREATING_TASKS"] do
    Process.send_after(self(), :get_status, @starting_status_interval)
  end

  defp schedule(_) do
    Process.send_after(self(), :get_status, @status_check_interval)
  end
end
