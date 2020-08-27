defmodule CogyntWorkstationIngest.Broadway.DrilldownProducer do
  use GenStage
  alias CogyntWorkstationIngest.Config
  alias KafkaEx.Protocol.Fetch

  # TODO: For now the DrilldownProducer is unused. It has been replaced with
  # the BroadwayKafka Producer. There should be no references to it in this
  # solution

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

  def flush_queue() do
    case Redis.key_exists?("des") do
      {:ok, false} ->
        Redis.hash_set("dmi", "tmc", 0)

      {:ok, true} ->
        {:ok, stream_length} = Redis.stream_length("des")
        Redis.stream_trim("des", 0)
        Redis.hash_increment_by("dmi", "tmc", -stream_length)
    end
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
    {messages, new_state} = fetch_demand_from_redis_stream("des", state)
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
    {messages, new_state} = fetch_demand_from_redis_stream("des", new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:retry_failed_messages, %{demand: 0} = state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info(:retry_failed_messages, state) do
    {messages, new_state} = fetch_demand_from_redis_stream("dfes", state)
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp parse_kafka_message_set(message_set) do
    # Incr the total message count that has been consumed for this event_definition
    message_count = Enum.count(message_set)
    Redis.hash_increment_by("dmi", "tmc", message_count)

    Enum.each(message_set, fn %Fetch.Message{value: json_message} ->
      case Jason.decode(json_message) do
        {:ok, message} ->
          Redis.stream_add(
            "des",
            "evt",
            %{
              event: message,
              retry_count: 0
            },
            compress: true
          )

        {:error, error} ->
          Redis.hash_increment_by("dmi", "tmc", -1)

          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to decode json_message. Error: #{inspect(error)}"
          )
      end
    end)
  end

  defp parse_failed_broadway_messages(broadway_messages) do
    Enum.each(broadway_messages, fn %Broadway.Message{
                                      data: %{
                                        event: message,
                                        retry_count: retry_count
                                      },
                                      status: status
                                    } ->
      CogyntLogger.error("#{__MODULE__}", "DrilldownEvent message failed. #{inspect(status)}")

      if retry_count < Config.drilldown_max_retry() do
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Failed messages retry. Attempt: #{retry_count + 1}"
        )

        Redis.stream_add(
          "dfes",
          "fld",
          %{
            event: message,
            retry_count: retry_count + 1
          },
          compress: true
        )
      end
    end)
  end

  defp fetch_demand_from_redis_stream(stream_name, %{demand: demand} = state) do
    if demand <= 0 do
      {[], state}
    else
      {:ok, stream_length} = Redis.stream_length(stream_name)

      stream_events =
        case stream_length >= demand do
          true ->
            # Read Stream by demand
            {:ok, stream_result} = Redis.stream_read(stream_name, demand)

            stream_events =
              Enum.flat_map(stream_result, fn [_, level_1] ->
                Enum.flat_map(level_1, fn [_, [_, value_2]] -> [:zlib.uncompress(value_2)] end)
              end)

            # Trim Stream by demand read
            Redis.stream_trim(stream_name, stream_length - demand)

            stream_events

          false ->
            if stream_length > 0 do
              # Read Stream by demand
              {:ok, stream_result} = Redis.stream_read(stream_name, stream_length)

              stream_events =
                Enum.flat_map(stream_result, fn [_, level_1] ->
                  Enum.flat_map(level_1, fn [_, [_, value_2]] -> [:zlib.uncompress(value_2)] end)
                end)

              Redis.stream_trim(stream_name, 0)

              stream_events
            else
              []
            end
        end

      case stream_events do
        [] ->
          {[], state}

        new_messages ->
          new_demand =
            case demand - Enum.count(new_messages) do
              val when val <= 0 ->
                0

              val ->
                val
            end

          new_state = Map.put(state, :demand, new_demand)

          {new_messages, new_state}
      end
    end
  end
end
