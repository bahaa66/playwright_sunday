defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor do
  defmacro __using__(opts) do
    supervisor_id = Keyword.get(opts, :supervisor_id)
    dimensions = Keyword.get(opts, :dimensions)
    brokers = Keyword.get(opts, :brokers)

    if is_nil(supervisor_id) do
      raise "You must provide a supervisor_id:\n\n  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,\n    supervisor_id: \"test_id\"\n\n"
    end

    if is_nil(dimensions) do
      raise "You must provide a list of dimensions:\n\n  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,\n    dimensions: [\"id\", \"test_dimension\"]\n\n"
    end

    if is_nil(dimensions) do
      raise "You must provide a brokers string:\n\n  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,\n    brokers: \"localhost:9092,localhost:9093\"\n\n"
    end

    quote do
      use GenServer

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
      def init(_arg) do
        Druid.status_health()
        |> case do
          {:ok, true} ->
            {:ok, %{supervisor_status: %{"state" => "LOADING"}},
             {:continue, {:create_or_update_supervisor, unquote(supervisor_id)}}}

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
      def handle_continue({:create_or_update_supervisor, id}, state) do
        brokers = unquote(brokers)
        dimensions = unquote(dimensions)
        supervisor_spec = Druid.Utils.base_kafka_supervisor(id, brokers, dimensions: dimensions)

        with {:ok, %{"id" => id}} <- Druid.create_or_update_supervisor(supervisor_spec),
             {:ok, %{"payload" => payload}} <- Druid.get_supervisor_status(id) do
          {:noreply, %{state | supervisor_status: payload}}
        else
          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to create and get supervisor information: #{inspect(error)}"
            )

            {:noreply, %{state | supervisor_status: {:error, error}}}
        end
      end
    end
  end
end
