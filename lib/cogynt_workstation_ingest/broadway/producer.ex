defmodule CogyntWorkstationIngest.Broadway.Producer do
  use GenStage
  require Logger
  alias KafkaEx.Protocol.Fetch

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_args) do
    {:producer, %{queues: %{}, demand: 0, failed_messages: []}}
  end

  def enqueue(message_set, event_definition, type) when is_list(message_set) do
    producer_name = type_to_name(type)
    GenServer.cast(producer_name, {:enqueue, message_set, event_definition})
  end

  def enqueue_failed_messages(broadway_messages, type) when is_list(broadway_messages) do
    producer_name = type_to_name(type)
    GenServer.cast(producer_name, {:enqueue_failed_messages, broadway_messages})
  end

  def drain_queue(event_definition_id, type) do
    producer_name = type_to_name(type)
    GenServer.cast(producer_name, {:drain_queue, event_definition_id})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast({:enqueue, message_set, event_definition}, %{queues: queues, demand: 0} = state) do
    queues = parse_kafka_message_set(message_set, event_definition, queues)
    new_state = Map.put(state, :queues, queues)
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast(
        {:enqueue, message_set, event_definition},
        %{queues: queues, demand: demand} = state
      ) do
    queues = parse_kafka_message_set(message_set, event_definition, queues)
    {messages, new_state} = fetch_and_release_demand(demand, queues, state)
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast(
        {:enqueue_failed_messages, broadway_messages},
        %{failed_messages: failed_messages} = state
      ) do
    failed_messages = parse_broadway_messages(broadway_messages, failed_messages)
    Process.send_after(self(), :tick, time_delay())
    new_state = Map.put(state, :failed_messages, failed_messages)
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:drain_queue, event_definition_id}, %{queues: queues} = state) do
    queues = Map.delete(queues, event_definition_id)
    new_state = Map.put(state, :queues, queues)
    {:noreply, [], new_state}
  end

  @impl true
  def handle_demand(incoming_demand, %{queues: queues, demand: demand} = state)
      when incoming_demand > 0 do
    Enum.each(queues, fn {id, queue} ->
      IO.puts("#{id}: Count Before: #{:queue.len(queue)}")
    end)

    IO.inspect(incoming_demand, label: "@@@ Incoming Demand")
    IO.inspect(demand, label: "@@@ Stored Demand")

    total_demand = incoming_demand + demand

    {messages, %{queues: queues} = new_state} =
      fetch_and_release_demand(total_demand, queues, state)

    Enum.each(queues, fn {id, queue} ->
      IO.puts("#{id}: Count After: #{:queue.len(queue)}")
    end)

    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(
        :tick,
        %{queues: queues, demand: demand, failed_messages: failed_messages} = state
      ) do
    queues = parse_failed_messages(failed_messages, queues)
    state = Map.put(state, :failed_messages, [])
    {messages, new_state} = fetch_and_release_demand(demand, queues, state)
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp parse_kafka_message_set(message_set, event_definition, queues) do
    Enum.reduce(message_set, queues, fn %Fetch.Message{value: json_message}, acc ->
      case Jason.decode(json_message) do
        {:ok, message} ->
          update_queue_value(acc, event_definition.id, %{
            event: message,
            event_definition: event_definition,
            retry_count: 0
          })

        {:error, error} ->
          Logger.error("Failed to decode json_message. Error: #{inspect(error)}")
          acc
      end
    end)
  end

  defp parse_broadway_messages(broadway_messages, failed_messages) do
    Enum.reduce(broadway_messages, failed_messages, fn %Broadway.Message{
                                                         data: %{
                                                           event: message,
                                                           event_definition: event_definition,
                                                           retry_count: retry_count
                                                         }
                                                       },
                                                       acc ->
      if retry_count < max_retry() do
        IO.puts("Retrying Failed Message, Id: #{event_definition.id}. Attempt: #{retry_count + 1}")

        acc ++
          [%{event: message, event_definition: event_definition, retry_count: retry_count + 1}]
      else
        acc
      end
    end)
  end

  defp parse_failed_messages(failed_messages, queues) do
    Enum.reduce(failed_messages, queues, fn %{event_definition: event_definition} = failed_message,
                                            acc ->
      update_queue_value(acc, event_definition.id, failed_message)
    end)
  end

  defp fetch_and_release_demand(demand, queues, state) when is_map(queues) do
    case Enum.empty?(queues) do
      true ->
        state = Map.put(state, :demand, demand)
        {[], state}

      false ->
        Enum.reduce_while(queues, {[], state}, fn {id, queue}, {acc_messages, acc_state} ->
          if Enum.count(acc_messages) <= 0,
            do: {
              :cont,
              fetch_and_release_demand(demand, queues, id, queue, acc_state)
            },
            else: {:halt, {acc_messages, acc_state}}
        end)
    end
  end

  defp fetch_and_release_demand(demand, queues, id, queue, state) do
    {items, queue} =
      case :queue.len(queue) >= demand do
        true ->
          :queue.split(demand, queue)

        false ->
          :queue.split(:queue.len(queue), queue)
      end

    case :queue.to_list(items) do
      [] ->
        queues = update_queue(queues, id, queue)

        new_state =
          Map.put(state, :queues, queues)
          |> Map.put(:demand, demand)

        {[], new_state}

      messages ->
        queues = update_queue(queues, id, queue)

        new_state =
          Map.put(state, :queues, queues)
          |> Map.put(:demand, demand - Enum.count(messages))

        {messages, new_state}
    end
  end

  defp update_queue_value(queues, event_definition_id, value) do
    queue =
      case Map.has_key?(queues, event_definition_id) do
        true ->
          Map.get(queues, event_definition_id)

        false ->
          :queue.new()
      end

    queue = :queue.in(value, queue)
    Map.put(queues, event_definition_id, queue)
  end

  defp update_queue(queues, event_definition_id, queue) do
    case Map.has_key?(queues, event_definition_id) do
      true ->
        Map.put(queues, event_definition_id, queue)

      false ->
        queues
    end
  end

  defp type_to_name(type) do
    default_name =
      case type do
        :linkevent ->
          :BroadwayLinkEventPipeline

        :event ->
          :BroadwayEventPipeline
      end

    producer_names = Broadway.producer_names(default_name)
    List.first(producer_names)
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp time_delay(), do: config()[:time_delay]
  defp max_retry(), do: config()[:max_retry]
end
