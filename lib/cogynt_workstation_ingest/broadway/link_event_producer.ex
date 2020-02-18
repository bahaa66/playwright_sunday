defmodule CogyntWorkstationIngest.Broadway.LinkEventProducer do
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
    {:producer, %{queue: :queue.new(), demand: 0, failed_messages: []}}
  end

  def enqueue(message_set, topic) when is_list(message_set) do
    process_names = Broadway.producer_names(String.to_atom("BroadwayLinkEventPipeline-#{topic}"))
    GenServer.cast(List.first(process_names), {:enqueue, message_set})
  end

  def enqueue_failed_messages(broadway_messages, topic) when is_list(broadway_messages) do
    process_names = Broadway.producer_names(String.to_atom("BroadwayLinkEventPipeline-#{topic}"))
    GenServer.cast(List.first(process_names), {:enqueue_failed_messages, broadway_messages})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast({:enqueue, message_set}, %{queue: queue, demand: 0} = state) do
    queue = parse_kafka_message_set(message_set, queue)
    # IO.inspect(queue, label: "@@@ Q after Enqueue")
    new_state = Map.put(state, :queue, queue)
    # IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:enqueue, message_set}, %{queue: queue, demand: demand} = state) do
    queue = parse_kafka_message_set(message_set, queue)

    # IO.inspect(queue, label: "@@@ Q after Enqueue")
    {messages, new_state} = fetch_and_release_demand(demand, queue, state)
    # IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_cast(
        {:enqueue_failed_messages, broadway_messages},
        %{queue: _queue, demand: _demand, failed_messages: failed_messages} = state
      ) do
    failed_messages = parse_broadway_messages(broadway_messages, failed_messages)
    IO.inspect(failed_messages, label: "@@@ Failed Messages")
    Process.send_after(__MODULE__, :tick, time_delay())
    new_state = Map.put(state, :failed_messages, failed_messages)
    IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_demand(incoming_demand, %{queue: queue, demand: demand} = state)
      when incoming_demand > 0 do
    IO.inspect(incoming_demand, label: "@@@ Incoming Demand")
    IO.inspect(demand, label: "@@@ Stored Demand")

    total_demand = incoming_demand + demand

    {messages, new_state} = fetch_and_release_demand(total_demand, queue, state)

    # IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_info(:tick, %{queue: queue, demand: demand, failed_messages: failed_messages} = state) do
    queue = parse_failed_messages(failed_messages, queue)

    # IO.inspect(queue, label: "@@@ Q after Enqueue")
    {messages, new_state} = fetch_and_release_demand(demand, queue, state)
    # IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp parse_kafka_message_set(message_set, queue) do
    Enum.reduce(message_set, queue, fn %Fetch.Message{value: json_message}, acc ->
      case Jason.decode(json_message) do
        {:ok, message} ->
          :queue.in(%{event: message, retry_count: 0}, acc)

        {:error, error} ->
          Logger.error("Failed to decode json_message. Error: #{inspect(error)}")
      end
    end)
  end

  defp parse_broadway_messages(broadway_messages, failed_messages) do
    Enum.reduce(broadway_messages, failed_messages, fn %Broadway.Message{
                                               data: %{event: message, retry_count: retry_count}
                                             },
                                             acc ->
      if retry_count < max_retry() do
        Logger.debug("INC Retry Count: #{retry_count + 1}")
        acc ++ [%{event: message, retry_count: retry_count + 1}]
      else
        acc
      end
    end)
  end

  defp parse_failed_messages(failed_messages, queue) do
    Enum.reduce(failed_messages, queue, fn failed_message, acc ->
      :queue.in(failed_message, acc)
    end)
  end

  defp fetch_and_release_demand(demand, queue, state) do
    {items, queue} =
      case :queue.len(queue) >= demand do
        true ->
          :queue.split(demand, queue)

        false ->
          :queue.split(:queue.len(queue), queue)
      end

    case :queue.to_list(items) do
      [] ->
        new_state =
          Map.put(state, :queue, queue)
          |> Map.put(:demand, demand)

        {[], new_state}

      messages ->
        new_state =
          Map.put(state, :queue, queue)
          |> Map.put(:demand, demand - Enum.count(messages))

        {messages, new_state}
    end
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp time_delay(), do: config()[:time_delay]
  defp max_retry(), do: config()[:max_retry]
end
