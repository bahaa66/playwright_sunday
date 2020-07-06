defmodule CogyntWorkstationIngest.Broadway.Producer do
  use GenStage
  alias KafkaEx.Protocol.Fetch
  alias CogyntWorkstationIngest.Config

  @defaults %{
    event_id: nil,
    retry_count: 0
  }
  @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_args) do
    {:producer, %{event_definition_ids: [], demand: 0}}
  end

  def enqueue(message_set, event_definition, type) when is_list(message_set) do
    producer_name = event_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue, message_set, event_definition})
  end

  def enqueue_failed_messages(broadway_messages, type) when is_list(broadway_messages) do
    producer_name = broadway_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue_failed_messages, broadway_messages})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast(
        {:enqueue, message_set, event_definition},
        %{event_definition_ids: event_definition_ids, demand: 0} = state
      ) do
    parse_kafka_message_set(message_set, event_definition)

    updated_event_definition_ids = Enum.uniq(event_definition_ids ++ [event_definition.id])

    new_state = Map.put(state, :event_definition_ids, updated_event_definition_ids)

    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast(
        {:enqueue, message_set, event_definition},
        %{event_definition_ids: event_definition_ids} = state
      ) do
    parse_kafka_message_set(message_set, event_definition)

    updated_event_definition_ids = Enum.uniq(event_definition_ids ++ [event_definition.id])

    new_state = Map.put(state, :event_definition_ids, updated_event_definition_ids)

    {messages, new_state} = fetch_and_release_demand(new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast({:enqueue_failed_messages, broadway_messages}, state) do
    parse_failed_broadway_messages(broadway_messages)
    Process.send_after(self(), :retry_failed_messages, Config.producer_time_delay())
    {:noreply, [], state}
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) when incoming_demand > 0 do
    total_demand = incoming_demand + demand
    new_state = Map.put(state, :demand, total_demand)
    {messages, new_state} = fetch_and_release_demand(new_state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:retry_failed_messages, state) do
    {messages, new_state} = fetch_and_release_failed_messages(state)
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  # Parse the %Fetch.Message{} struct returned from Kafka and
  # encode the event_data and append it to the List in Redis
  defp parse_kafka_message_set(message_set, event_definition) do
    # Incr the total message count that has been consumed for this event_definition
    message_count = Enum.count(message_set)
    Redis.hash_increment_by("b:#{event_definition.id}", "tmc", message_count)

    list_items =
      Enum.reduce(message_set, [], fn %Fetch.Message{value: json_message}, acc ->
        case Jason.decode(json_message) do
          {:ok, message} ->
            acc ++
              [
                %{
                  event: message,
                  event_definition: event_definition,
                  event_id: @defaults.event_id,
                  retry_count: @defaults.retry_count
                }
              ]

          {:error, error} ->
            Redis.hash_increment_by("b:#{event_definition.id}", "tmc", -1)

            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to decode json_message. Error: #{inspect(error, pretty: true)}"
            )
        end
      end)

    Redis.list_append_pipeline("a:#{event_definition.id}", list_items)
  end

  # Parse the %Broadway.Message{} struct returned from Broadway
  # pipeline and encode the failed_messages and append it to the List
  # in Redis
  defp parse_failed_broadway_messages(broadway_messages) do
    Enum.each(broadway_messages, fn %Broadway.Message{
                                      data: %{
                                        event: message,
                                        event_definition: event_definition,
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
          "Retrying Failed Message, Id: #{event_definition.id}. Attempt: #{retry_count + 1}"
        )

        Redis.list_append("a:failed_messages", %{
          event: message,
          event_definition: event_definition,
          event_id: event_id,
          retry_count: retry_count + 1
        })
      end
    end)
  end

  # Takes the stored demand in the Producer and pulls an even amount of messages
  # from each event_definition that is actively trying to Ingest data.
  defp fetch_and_release_demand(
         %{event_definition_ids: event_definition_ids, demand: demand} = state
       )
       when is_list(event_definition_ids) do
    case Enum.empty?(event_definition_ids) do
      true ->
        {[], state}

      false ->
        fetch_count = div(demand, Enum.count(event_definition_ids))
        new_state = {[], state}

        Enum.reduce(event_definition_ids, new_state, fn event_definition_id,
                                                        {acc_messages, acc_state} ->
          fetch_demand_from_redis(fetch_count, event_definition_id, acc_messages, acc_state)
        end)
    end
  end

  # TODO: Need to work on maintaining of the failed messages list
  # Will fetch the event_data from Redis list based on the fetch_count or
  # the size of the Redis List
  defp fetch_demand_from_redis(
         fetch_count,
         event_definition_id,
         messages,
         %{event_definition_ids: event_definition_ids, demand: demand} = state
       ) do
    {:ok, list_length} = Redis.list_length("a:#{event_definition_id}")

    {list_items, updated_event_definition_id_list} =
      case list_length >= fetch_count do
        true ->
          # Get List Range by fetch_count
          {:ok, list_items} = Redis.list_range("a:#{event_definition_id}", 0, fetch_count - 1)

          # Trim List Range by fetch_count
          Redis.list_trim("a:#{event_definition_id}", fetch_count, 100_000_000)

          {list_items, event_definition_ids}

        false ->
          # Get List Range by list_length
          {:ok, list_items} = Redis.list_range("a:#{event_definition_id}", 0, list_length - 1)

          # Trim List Range by list_length
          Redis.list_trim("a:#{event_definition_id}", list_length, -1)

          # There is no more data left to process. Remove from the list
          updated_event_definition_id_list =
            List.delete(event_definition_ids, event_definition_id)

          {list_items, updated_event_definition_id_list}
      end

    case list_items do
      [] ->
        {messages, state}

      new_messages ->
        new_state =
          Map.put(state, :event_definition_ids, updated_event_definition_id_list)
          |> Map.put(:demand, demand - Enum.count(new_messages))

        {messages ++ new_messages, new_state}
    end
  end

  # Will fetch the failed_messages from Redis list based on the demand
  # or the size of the Redis List
  defp fetch_and_release_failed_messages(%{demand: demand} = state) do
    {:ok, list_length} = Redis.list_length("a:failed_messages")

    list_items =
      case list_length >= demand do
        true ->
          # Get List Range by demand
          {:ok, list_items} = Redis.list_range("a:failed_messages", 0, demand - 1)

          # Trim List Range by demand
          Redis.list_trim("a:failed_messages", demand, 100_000_000)

          list_items

        false ->
          # Get List Range by list_length
          {:ok, list_items} = Redis.list_range("a:failed_messages", 0, list_length - 1)

          # Trim List Range by list_length
          Redis.list_trim("a:failed_messages", list_length, -1)

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

  defp event_type_to_name(event_type) do
    default_name =
      case event_type do
        @linkage ->
          :BroadwayLinkEventPipeline

        _ ->
          :BroadwayEventPipeline
      end

    producer_names = Broadway.producer_names(default_name)
    List.first(producer_names)
  end

  defp broadway_type_to_name(broadway_type) do
    producer_names = Broadway.producer_names(broadway_type)
    List.first(producer_names)
  end
end
