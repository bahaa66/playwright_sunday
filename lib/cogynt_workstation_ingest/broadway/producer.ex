defmodule CogyntWorkstationIngest.Broadway.Producer do
  use GenStage
  alias KafkaEx.Protocol.Fetch
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient

  @defaults %{
    event_processed: false,
    event_id: nil,
    retry_count: 0,
    delete_ids: nil,
    delete_docs: nil
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
    {:producer, %{queues: %{}, demand: 0, failed_messages: []}}
  end

  def enqueue(message_set, event_definition, type) when is_list(message_set) do
    producer_name = event_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue, message_set, event_definition})
  end

  def enqueue_failed_messages(broadway_messages, type) when is_list(broadway_messages) do
    producer_name = broadway_type_to_name(type)
    GenServer.cast(producer_name, {:enqueue_failed_messages, broadway_messages})
  end

  def is_processing?(event_definition_id, type) do
    producer_name = event_type_to_name(type)
    GenServer.call(producer_name, {:is_processing, event_definition_id})
  end

  def drain_queue(event_definition_id, type) do
    producer_name = event_type_to_name(type)
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
    IO.inspect(Enum.count(messages), label: "@@@ MESSAGE COUNT")
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
  def handle_call({:is_processing, event_definition_id}, _from, %{queues: queues} = state) do
    is_processing =
      case Map.has_key?(queues, event_definition_id) do
        true ->
          queue = Map.get(queues, event_definition_id)

          if :queue.len(queue) > 0 do
            true
          else
            false
          end

        false ->
          false
      end

    {:reply, is_processing, [], state}
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
    IO.inspect(incoming_demand, label: "@@@ Incoming Demand")
    IO.inspect(demand, label: "@@@ Demand")
    total_demand = incoming_demand + demand
    {messages, new_state} = fetch_and_release_demand(total_demand, queues, state)
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
            event_processed: @defaults.event_processed,
            event_id: @defaults.event_id,
            retry_count: @defaults.retry_count,
            delete_ids: @defaults.delete_ids,
            delete_docs: @defaults.delete_docs
          })

        {:error, error} ->
          CogyntLogger.error(
            "Producer",
            "Failed to decode json_message. Error: #{inspect(error)}"
          )

          acc
      end
    end)
  end

  defp parse_broadway_messages(broadway_messages, failed_messages) do
    Enum.reduce(broadway_messages, failed_messages, fn %Broadway.Message{
                                                         data: %{
                                                           event: message,
                                                           event_definition: event_definition,
                                                           event_processed: processed,
                                                           event_id: event_id,
                                                           retry_count: retry_count,
                                                           delete_ids: delete_event_ids,
                                                           delete_docs: delete_doc_ids
                                                         }
                                                       },
                                                       acc ->
      if retry_count < max_retry() do
        IO.puts(
          "Retrying Failed Message, Id: #{event_definition.id}. Attempt: #{retry_count + 1}"
        )

        acc ++
          [
            %{
              event: message,
              event_definition: event_definition,
              event_processed: processed,
              event_id: event_id,
              retry_count: retry_count + 1,
              delete_ids: delete_event_ids,
              delete_docs: delete_doc_ids
            }
          ]
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

  defp fetch_and_release_demand(total_demand, queues, state) when is_map(queues) do
    case Enum.empty?(queues) do
      true ->
        state = Map.put(state, :demand, total_demand)
        {[], state}

      false ->
        fetch_count = div(total_demand, Enum.count(queues))
        state = Map.put(state, :demand, total_demand)
        new_state = {[], state}

        Enum.reduce(queues, new_state, fn {id, queue}, {acc_messages, acc_state} ->
          fetch_demand_per_queue(fetch_count, queues, id, queue, acc_messages, acc_state)
        end)
    end
  end

  defp fetch_demand_per_queue(
         fetch_count,
         queues,
         id,
         queue,
         messages,
         %{demand: demand} = state
       ) do
    IO.inspect(:queue.len(queue), label: "@@@ SIZE")

    {items, queue} =
      case :queue.len(queue) >= fetch_count do
        true ->
          :queue.split(fetch_count, queue)

        false ->
          :queue.split(:queue.len(queue), queue)
      end

    case :queue.to_list(items) do
      [] ->
        {messages, state}

      new_messages ->
        queues =
          case :queue.len(queue) == 0 do
            true ->
              CogyntClient.publish_consumer_status(id, nil)
              Map.delete(queues, id)

            false ->
              update_queue(queues, id, queue)
          end

        new_state =
          Map.put(state, :queues, queues)
          |> Map.put(:demand, demand - Enum.count(messages))

        {messages ++ new_messages, new_state}
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

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp time_delay(), do: config()[:time_delay]
  defp max_retry(), do: config()[:max_retry]
end
