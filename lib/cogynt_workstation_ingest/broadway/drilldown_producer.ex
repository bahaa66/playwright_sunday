defmodule CogyntWorkstationIngest.Broadway.DrilldownProducer do
  use GenStage
  alias CogyntWorkstationIngest.Config
  alias KafkaEx.Protocol.Fetch

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_args) do
    {:producer, %{demand: 0}}
  end

  def enqueue(message_set) when is_list(message_set) do
    process_names = Broadway.producer_names(:BroadwayDrilldown)
    GenServer.cast(List.first(process_names), {:enqueue, message_set})
  end

  def enqueue_failed_messages(broadway_messages) when is_list(broadway_messages) do
    process_names = Broadway.producer_names(:BroadwayDrilldown)
    GenServer.cast(List.first(process_names), {:enqueue_failed_messages, broadway_messages})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast({:enqueue, message_set}, %{demand: 0} = state) do
    parse_kafka_message_set(message_set)
    {:noreply, [], state}
  end

  @impl true
  def handle_cast({:enqueue, message_set}, state) do
    parse_kafka_message_set(message_set)
    {messages, new_state} = fetch_demand_from_redis(state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast({:enqueue_failed_messages, broadway_messages}, state) do
    parse_failed_broadway_messages(broadway_messages)
    Process.send_after(self(), :retry_failed_messages, Config.drilldown_time_delay())
    {:noreply, [], state}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) when incoming_demand > 0 do
    total_demand = incoming_demand + demand
    new_state = Map.put(state, :demand, total_demand)
    {messages, new_state} = fetch_demand_from_redis(new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:retry_failed_messages, state) do
    {messages, new_state} = fetch_and_release_failed_messages_from_redis(state)
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp parse_kafka_message_set(message_set) do
    # Incr the total message count that has been consumed for this event_definition
    message_count = Enum.count(message_set)
    Redis.hash_increment_by("drilldown_message_info", "tmc", message_count)

    list_items =
      Enum.reduce(message_set, [], fn %Fetch.Message{value: json_message}, acc ->
        case Jason.decode(json_message) do
          {:ok, message} ->
            acc ++
              [
                %{
                  event: message,
                  retry_count: 0
                }
              ]

          {:error, error} ->
            Redis.hash_increment_by("drilldown_message_info", "tmc", -1)

            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to decode json_message. Error: #{inspect(error)}"
            )

            acc
        end
      end)

    Redis.list_append_pipeline("drilldown_event_messages", list_items)
  end

  defp parse_failed_broadway_messages(broadway_messages) do
    Enum.each(broadway_messages, fn %Broadway.Message{
                                      data: %{
                                        event: message,
                                        retry_count: retry_count
                                      },
                                      status: status
                                    } ->
      CogyntLogger.error("#{__MODULE__}", "Event message failed. #{inspect(status)}")

      if retry_count < Config.drilldown_max_retry() do
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Failed messages retry. Attempt: #{retry_count + 1}"
        )

        Redis.list_append("drilldown_failed_messages", %{
          event: message,
          retry_count: retry_count + 1
        })
      end
    end)
  end

  defp fetch_demand_from_redis(%{demand: demand} = state) do
    {:ok, list_length} = Redis.list_length("drilldown_event_messages")

    list_items =
      case list_length >= demand do
        true ->
          # Get List Range by fetch_count
          {:ok, list_items} = Redis.list_range("drilldown_event_messages", 0, demand - 1)

          # Trim List Range by fetch_count
          Redis.list_trim("drilldown_event_messages", demand, 100_000_000)

          list_items

        false ->
          # Get List Range by list_length
          {:ok, list_items} = Redis.list_range("drilldown_event_messages", 0, list_length - 1)

          # Trim List Range by list_length
          Redis.list_trim("drilldown_event_messages", list_length, -1)

          list_items
      end

    case list_items do
      [] ->
        {[], state}

      new_messages ->
        new_state = Map.put(state, :demand, demand - Enum.count(new_messages))

        {new_messages, new_state}
    end
  end

  defp fetch_and_release_failed_messages_from_redis(%{demand: demand} = state) do
    {:ok, list_length} = Redis.list_length("drilldown_failed_messages")

    list_items =
      case list_length >= demand do
        true ->
          # Get List Range by fetch_count
          {:ok, list_items} = Redis.list_range("drilldown_failed_messages", 0, demand - 1)

          # Trim List Range by fetch_count
          Redis.list_trim("drilldown_failed_messages", demand, 100_000_000)

          list_items

        false ->
          # Get List Range by list_length
          {:ok, list_items} = Redis.list_range("drilldown_failed_messages", 0, list_length - 1)

          # Trim List Range by list_length
          Redis.list_trim("drilldown_failed_messages", list_length, -1)

          list_items
      end

    case list_items do
      [] ->
        {[], state}

      new_messages ->
        new_state = Map.put(state, :demand, demand - Enum.count(new_messages))

        {new_messages, new_state}
    end
  end
end
