defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor do
  defmacro __using__(opts) do
    supervisor_id = Keyword.get(opts, :supervisor_id)
    dimensions_spec = Keyword.get(opts, :dimensions_spec)
    brokers = Keyword.get(opts, :brokers)
    io_config = Keyword.get(opts, :io_config)
    granularity_spec = Keyword.get(opts, :granularity_spec)
    timestamp_spec = Keyword.get(opts, :timestamp_spec)

    if is_nil(supervisor_id) do
      raise "You must provide a supervisor_id:\n\n  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,\n    supervisor_id: \"test_id\"\n\n"
    end

    if is_nil(brokers) do
      raise "You must provide a brokers string:\n\n  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,\n    brokers: \"localhost:9092,localhost:9093\"\n\n"
    end

    quote do
      use GenServer

      @status_check_interval 30_000

      # -------------------- #
      # --- client calls --- #
      # -------------------- #
      def start_link do
        GenServer.start_link(__MODULE__, [], name: __MODULE__)
      end

      def supervisor_status do
        GenServer.call(__MODULE__, :supervisor_status)
      end

      def healthy? do
        GenServer.call(__MODULE__, :healthy?)
      end

      def state do
        GenServer.call(__MODULE__, :state)
      end

      def delete_data_and_reset_supervisor do
        GenServer.call(__MODULE__, :delete_data_and_reset_supervisor)
      end

      # ------------------------ #
      # --- server callbacks --- #
      # ------------------------ #
      @impl true
      def init(_arg) do
        Druid.status_health()
        |> case do
          {:ok, true} ->
            {:ok, %{id: unquote(supervisor_id), supervisor_status: %{"state" => "LOADING"}},
             {:continue, :create_or_update_supervisor}}

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
              "Unable to delete datasource do to error: #{inspect(error)}"
            )

            {:reply, {:error, error}, state}
        end
      end

      @impl true
      def handle_continue(:create_or_update_supervisor, %{id: id} = state) do
        brokers = unquote(brokers)
        dimensions_spec = unquote(dimensions_spec)
        io_config = unquote(io_config)
        granularity_spec = unquote(granularity_spec)
        timestamp_spec = unquote(timestamp_spec)

        supervisor_spec =
          Druid.Utils.build_kafka_supervisor(id, brokers,
            dimensions_spec: dimensions_spec,
            io_config: io_config,
            granularity_spec: granularity_spec,
            timestamp_spec: timestamp_spec
          )

        with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
             {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
          Process.send_after(__MODULE__, :update_status, @status_check_interval)
          {:noreply, %{state | supervisor_status: payload}}
        else
          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to create and get supervisor information for #{id}: #{inspect(error)}"
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
              "Unable to reset and get supervisor information for #{id}: #{inspect(error)}"
            )

            {:noreply, state}
        end
      end

      @impl true
      def handle_info(:update_status, %{id: id} = state) do
        Druid.get_supervisor_status(id)
        |> case do
          {:ok, %{"payload" => %{"detailedState" => "LOST_CONTACT_WITH_STREAM"} = p}} ->
            {:noreply, state, {:continue, :reset_and_get_supervisor}}

          {:ok, %{"payload" => payload} = s} ->
            Process.send_after(__MODULE__, :update_status, @status_check_interval)
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
  end
end
