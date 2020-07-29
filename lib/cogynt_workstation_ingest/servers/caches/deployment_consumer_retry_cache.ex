defmodule CogyntWorkstationIngest.Servers.Caches.DeploymentConsumerRetryCache do
  @moduledoc """
  Module that will retry to create the deployment consumer. Will retry
  for the max amount of retries in the given interval of the configurations.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(
      __MODULE__,
      [
        {:ets_table_name, :deployment_consumer_retry_cache},
        {:log_limit, 500_000}
      ],
      name: __MODULE__
    )
  end

  def retry_consumer(topic) do
    GenServer.cast(__MODULE__, {:retry_consumer, topic})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(args) do
    [{:ets_table_name, table_name}, {:log_limit, log_limit}] = args
    :ets.new(table_name, [:named_table, :set])

    timer_ref =
      Process.send_after(__MODULE__, :tick, Config.deployment_consumer_retry_time_delay())

    {:ok,
     %{
       timer: timer_ref,
       log_limit: log_limit,
       ets_table_name: table_name
     }}
  end

  @impl true
  def handle_cast(
        {:retry_consumer, topic},
        %{ets_table_name: table_name} = state
      ) do
    case :ets.lookup(table_name, topic) do
      [] ->
        true = :ets.insert(table_name, {topic, 0})

      [{key, count}] ->
        if count < Config.deployment_consumer_retry_max_retry() do
          true = :ets.insert(table_name, {key, count + 1})
        else
          true = :ets.delete(table_name, key)
        end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %{ets_table_name: table_name} = state) do
    timer_ref =
      Process.send_after(__MODULE__, :tick, Config.deployment_consumer_retry_time_delay())

    # Grab all the records in the ets table and put them into a list
    ets_records = :ets.tab2list(table_name)

    Enum.each(ets_records, fn {topic, value_count} ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Retrying to create Consumer: #{topic}"
      )

      case ConsumerGroupSupervisor.start_child(topic) do
        {:error, nil} ->
          if value_count < Config.deployment_consumer_retry_max_retry() do
            true = :ets.insert(table_name, {topic, value_count + 1})
          else
            true = :ets.delete(table_name, topic)
          end

          CogyntLogger.warn("#{__MODULE__}", "Deployment Topic DNE. Adding to RetryCache")

        _ ->
          true = :ets.delete(table_name, topic)
          CogyntLogger.info("#{__MODULE__}", "Started Deployment Stream")
      end
    end)

    {:noreply, %{state | timer: timer_ref}}
  end
end
