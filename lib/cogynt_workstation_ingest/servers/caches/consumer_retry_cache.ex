defmodule CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache do
  @moduledoc """
  Module that will retry to create consumers that did not have topics on Kafka
  at the original time of creation. Will retry for the max amount of retries in the
  given interval of the configurations.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.Servers.ConsumerStateManager

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(
      __MODULE__,
      [
        {:ets_table_name, :consumer_retry_cache},
        {:log_limit, 500_000}
      ],
      name: __MODULE__
    )
  end

  def retry_consumer(event_definition) do
    GenServer.cast(__MODULE__, {:retry_consumer, event_definition})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(args) do
    [{:ets_table_name, table_name}, {:log_limit, log_limit}] = args
    :ets.new(table_name, [:named_table, :set])

    timer_ref = Process.send_after(__MODULE__, :tick, Config.consumer_retry_time_delay())

    {:ok,
     %{
       timer: timer_ref,
       log_limit: log_limit,
       ets_table_name: table_name
     }}
  end

  @impl true
  def handle_cast(
        {:retry_consumer, event_definition},
        %{ets_table_name: table_name} = state
      ) do
    case :ets.lookup(table_name, event_definition) do
      [] ->
        true = :ets.insert(table_name, {event_definition, 0})

      [{key, count}] ->
        if count < Config.consumer_retry_max_retry() do
          true = :ets.insert(table_name, {key, count + 1})
        else
          true = :ets.delete(table_name, key)
        end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %{ets_table_name: table_name} = state) do
    timer_ref = Process.send_after(__MODULE__, :tick, Config.consumer_retry_time_delay())

    # Grab all the records in the ets table and put them into a list
    ets_records = :ets.tab2list(table_name)

    Enum.each(ets_records, fn {event_definition, _value_count} ->
      CogyntLogger.info(
        "#{__MODULE__}",
        "Retrying to create Consumer: #{event_definition.topic}"
      )

      with %EventDefinition{} = new_ed <- EventsContext.get_event_definition(event_definition.id),
           true <- is_nil(new_ed.deleted_at),
           true <- new_ed.active do
        ConsumerStateManager.manage_request(%{start_consumer: event_definition})
      else
        _ -> nil
      end
    end)

    {:noreply, %{state | timer: timer_ref}}
  end
end
