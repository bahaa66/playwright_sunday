defmodule CogyntWorkstationIngest.Broadway.Producer do
  use GenStage
  alias KafkaEx.Protocol.Fetch
  alias CogyntWorkstationIngest.Config

  @defaults %{
    event_id: nil,
    retry_count: 0
  }
  @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]
  @link_pipeline_name :BroadwayLinkEventPipeline
  @event_pipeline_name :BroadwayEventPipeline

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(args) do
    broadway = Keyword.get(args, :broadway)
    name = Keyword.get(broadway, :name)

    # Trap exit
    Process.flag(:trap_exit, true)

    case name do
      @link_pipeline_name ->
        case Redis.hash_get("lepk", "edid", decode: true) do
          {:ok, nil} ->
            {:producer, %{event_definition_ids: [], name: "lep", demand: 0}}

          {:ok, event_definition_ids} ->
            {:producer,
             %{
               event_definition_ids: event_definition_ids,
               name: "lep",
               demand: 0
             }}
        end

      @event_pipeline_name ->
        case Redis.hash_get("epk", "edid", decode: true) do
          {:ok, nil} ->
            {:producer, %{event_definition_ids: [], name: "ep", demand: 0}}

          {:ok, event_definition_ids} ->
            {:producer, %{event_definition_ids: event_definition_ids, name: "ep", demand: 0}}
        end
    end
  end

  def enqueue(message_set, event_definition_id, type) when is_list(message_set) do
    producer_name = event_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue, message_set, event_definition_id})
  end

  def enqueue_failed_messages(broadway_messages, type) when is_list(broadway_messages) do
    producer_name = broadway_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue_failed_messages, broadway_messages})
  end

  def flush_queue(event_definition_id) do
    case Redis.key_exists?("a:#{event_definition_id}") do
      {:ok, false} ->
        Redis.hash_set("b:#{event_definition_id}", "tmc", 0)

      {:ok, true} ->
        {:ok, stream_length} = Redis.stream_length("a:#{event_definition_id}")
        Redis.key_delete("a:#{event_definition_id}")
        # TODO: remove failed events ??f
        Redis.hash_increment_by("b:#{event_definition_id}", "tmc", -stream_length)
    end
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast(
        {:enqueue, message_set, event_definition_id},
        %{event_definition_ids: event_definition_ids, name: name, demand: 0} = state
      ) do
    parse_kafka_message_set(message_set, event_definition_id)

    updated_event_definition_ids = Enum.uniq(event_definition_ids ++ [event_definition_id])
    Redis.hash_set(name <> "k", "edid", updated_event_definition_ids)
    new_state = Map.put(state, :event_definition_ids, updated_event_definition_ids)

    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast(
        {:enqueue, message_set, event_definition_id},
        %{event_definition_ids: event_definition_ids, name: name} = state
      ) do
    parse_kafka_message_set(message_set, event_definition_id)

    updated_event_definition_ids = Enum.uniq(event_definition_ids ++ [event_definition_id])
    new_state = Map.put(state, :event_definition_ids, updated_event_definition_ids)
    Redis.hash_set(name <> "k", "edid", updated_event_definition_ids)

    {messages, new_state} = fetch_and_release_demand(new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast({:enqueue_failed_messages, broadway_messages}, %{name: name} = state) do
    parse_failed_broadway_messages(broadway_messages, name)
    Process.send_after(self(), :retry_failed_messages, Config.producer_time_delay())
    {:noreply, [], state}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state)
      when incoming_demand > 0 do
    total_demand = incoming_demand + demand
    new_state = Map.put(state, :demand, total_demand)
    {messages, new_state} = fetch_and_release_demand(new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:retry_failed_messages, %{demand: 0} = state) do
    {:noreply, [], state}
  end

  @impl true
  def handle_info(:retry_failed_messages, state) do
    {messages, new_state} = fetch_and_release_failed_messages(state)

    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(
        {:EXIT, _pid, :normal},
        %{event_definition_ids: event_definition_ids, name: name} = state
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "GenStage Producer being shutdown... persiting state to Redis"
    )

    # persist state to redis
    Redis.hash_set(name <> "k", "event_definition_ids", event_definition_ids)
    {:noreply, state}
  end

  # Callback that will persist data to the filesystem before the server shuts down
  @impl true
  def terminate(reason, %{event_definition_ids: event_definition_ids, name: name} = state) do
    CogyntLogger.warn(
      "#{__MODULE__}",
      "GenStage Producer crashed for the following reason: #{inspect(reason, pretty: true)}... persiting state to Redis"
    )

    # persist state to redis
    Redis.hash_set(name <> "k", "edid", event_definition_ids)
    {:stop, reason, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  # Parse the %Fetch.Message{} struct returned from Kafka and
  # encode the event_data and append it to the List in Redis
  defp parse_kafka_message_set(message_set, event_definition_id) do
    # Incr the total message count that has been consumed for this event_definition
    message_count = Enum.count(message_set)
    Redis.hash_increment_by("b:#{event_definition_id}", "tmc", message_count)

    Enum.each(message_set, fn %Fetch.Message{value: json_message} ->
      case Jason.decode(json_message) do
        {:ok, message} ->
          Redis.stream_add(
            "a:#{event_definition_id}",
            "evt",
            %{
              event: message,
              event_definition_id: event_definition_id,
              event_id: @defaults.event_id,
              retry_count: @defaults.retry_count
            },
            compress: true
          )

        {:error, error} ->
          Redis.hash_increment_by("b:#{event_definition_id}", "tmc", -1)

          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to decode json_message. Error: #{inspect(error, pretty: true)}"
          )
      end
    end)
  end

  # Parse the %Broadway.Message{} struct returned from Broadway
  # pipeline and encode the failed_messages and append it to the List
  # in Redis
  defp parse_failed_broadway_messages(broadway_messages, name) do
    Enum.each(broadway_messages, fn %Broadway.Message{
                                      data: %{
                                        event: message,
                                        event_definition_id: event_definition_id,
                                        event_id: event_id,
                                        retry_count: retry_count
                                      },
                                      status: status
                                    } ->
      CogyntLogger.error(
        "#{__MODULE__}",
        "Event message failed. #{inspect(status, pretty: true)}"
      )

      if retry_count < Config.producer_max_retry() do
        CogyntLogger.info(
          "#{__MODULE__}",
          "Retrying Failed Message, Id: #{event_definition_id}. Attempt: #{retry_count + 1}"
        )

        Redis.stream_add(
          name <> "fes",
          "fld",
          %{
            event: message,
            event_definition_id: event_definition_id,
            event_id: event_id,
            retry_count: retry_count + 1
          },
          compress: true
        )
      end
    end)
  end

  # Takes the stored demand in the Producer and pulls an even amount of messages
  # from each event_definition that is actively trying to Ingest data.
  defp fetch_and_release_demand(
         %{event_definition_ids: event_definition_ids, demand: demand} = state
       )
       when is_list(event_definition_ids) and demand > 0 do
    case Enum.empty?(event_definition_ids) do
      true ->
        {[], state}

      false ->
        fetch_count = div(demand, Enum.count(event_definition_ids))

        if fetch_count <= 0 do
          {[], state}
        else
          new_state = {[], state}

          Enum.reduce(event_definition_ids, new_state, fn event_definition_id,
                                                          {acc_messages, acc_state} ->
            fetch_demand_from_redis_stream(
              "a:#{event_definition_id}",
              fetch_count,
              event_definition_id,
              acc_messages,
              acc_state
            )
          end)
        end
    end
  end

  defp fetch_demand_from_redis_stream(
         stream_name,
         fetch_count,
         event_definition_id,
         messages,
         %{event_definition_ids: event_definition_ids, name: name, demand: demand} = state
       ) do
    {:ok, stream_length} = Redis.stream_length(stream_name)

    {stream_events, updated_event_definition_ids} =
      case stream_length >= fetch_count do
        true ->
          # Read Stream by demand
          {:ok, stream_result} = Redis.stream_read(stream_name, fetch_count)

          stream_events =
            Enum.flat_map(stream_result, fn [_, level_1] ->
              Enum.flat_map(level_1, fn [_, [_, value_2]] -> [:zlib.uncompress(value_2)] end)
            end)

          # Trim Stream by demand read
          Redis.stream_trim(stream_name, stream_length - demand)

          {stream_events, event_definition_ids}

        false ->
          # There is no more data left to process. Remove from the list
          updated_event_definition_ids = List.delete(event_definition_ids, event_definition_id)

          Redis.hash_set(name <> "k", "edid", updated_event_definition_ids)

          if stream_length > 0 do
            # Read Stream by demand
            {:ok, stream_result} = Redis.stream_read(stream_name, stream_length)

            stream_events =
              Enum.flat_map(stream_result, fn [_, level_1] ->
                Enum.flat_map(level_1, fn [_, [_, value_2]] -> [:zlib.uncompress(value_2)] end)
              end)

            Redis.stream_trim(stream_name, 0)

            {stream_events, updated_event_definition_ids}
          else
            {[], updated_event_definition_ids}
          end
      end

    case stream_events do
      [] ->
        new_state = Map.put(state, :event_definition_ids, updated_event_definition_ids)
        {messages, new_state}

      new_messages ->
        new_demand =
          case demand - Enum.count(new_messages) do
            val when val <= 0 ->
              0

            val ->
              val
          end

        new_state =
          Map.put(state, :event_definition_ids, updated_event_definition_ids)
          |> Map.put(:demand, new_demand)

        {messages ++ new_messages, new_state}
    end
  end

  # Will fetch the failed_messages from Redis list based on the demand
  # or the size of the Redis Stream
  defp fetch_and_release_failed_messages(%{demand: demand, name: name} = state) do
    stream_name = name <> "fes"

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

  defp event_type_to_name(event_type) do
    default_name =
      case event_type do
        @linkage ->
          @link_pipeline_name

        _ ->
          @event_pipeline_name
      end

    producer_names = Broadway.producer_names(default_name)
    List.first(producer_names)
  end

  defp broadway_type_to_name(broadway_type) do
    producer_names = Broadway.producer_names(broadway_type)
    List.first(producer_names)
  end
end
