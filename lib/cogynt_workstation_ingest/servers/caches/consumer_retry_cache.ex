defmodule CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache do
  @moduledoc """
  Module that will retry to create consumers that did not have topics on Kafka
  at the original time of creation. Will retry for the max amount of retries in the
  given interval of the configurations.
  """
  use GenServer
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Events.EventDefinition

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

    timer_ref = Process.send_after(__MODULE__, :tick, time_delay())

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
        if count < max_retry() do
          true = :ets.insert(table_name, {key, count + 1})
        else
          true = :ets.delete(table_name, key)
        end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %{ets_table_name: table_name} = state) do
    timer_ref = Process.send_after(__MODULE__, :tick, time_delay())

    # Grab all the records in the ets table and put them into a list
    ets_records = :ets.tab2list(table_name)

    Enum.each(ets_records, fn {event_definition, _value_count} ->
      CogyntLogger.info(
        "Topic Does Not Exist",
        "Retrying to create Consumer: #{event_definition.topic}"
      )

      with %EventDefinition{} = new_ed <- EventsContext.get_event_definition(event_definition.id),
           true <- is_nil(new_ed.deleted_at),
           {:ok, _pid} <- ConsumerGroupSupervisor.start_child(event_definition) do
        CogyntClient.publish_consumer_status(
          event_definition.id,
          event_definition.topic,
          ConsumerStatusTypeEnum.running()
        )
      else
        _ -> nil
      end
    end)

    {:noreply, %{state | timer: timer_ref}}
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp time_delay(), do: config()[:time_delay]
  defp max_retry(), do: config()[:max_retry]
end
