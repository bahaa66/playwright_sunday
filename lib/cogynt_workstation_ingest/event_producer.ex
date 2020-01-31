defmodule CogyntWorkstationIngest.EventProducer do
  use GenStage
  require Logger
  alias KafkaEx.Protocol.Fetch.Message

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_args) do
    {:producer, %{queue: :queue.new(), demand: 0}}
  end

  def enqueue(message_set) when is_list(message_set) do
    # TODO figure out how to call specific name of Broadway producer for callbacks
    GenServer.cast(
      :"Elixir.CogyntWorkstationIngest.EventPipelineTEST.Broadway.Producer_0",
      {:enqueue, message_set}
    )
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast({:enqueue, message_set}, %{queue: queue, demand: 0} = state) do
    queue =
      Enum.reduce(message_set, queue, fn json_message, acc ->
        case Jason.decode(json_message) do
          {:ok, message} ->
            :queue.in(message, acc)

          {:error, error} ->
            Logger.error("Failed to decode json_message. Error: #{inspect(error)}")
        end
      end)

    new_state = Map.put(state, :queue, queue)
    IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, [], new_state}
  end

  @impl true
  def handle_cast({:enqueue, message_set}, %{queue: queue, demand: demand} = state) do
    queue =
      Enum.reduce(message_set, queue, fn json_message, acc ->
        case Jason.decode(json_message) do
          {:ok, message} ->
            :queue.in(message, acc)

          {:error, error} ->
            Logger.error("Failed to decode json_message. Error: #{inspect(error)}")
        end
      end)

    IO.inspect(queue, label: "@@@ Q after Enqueue")
    {messages, new_state} = fetch_and_release_demand(demand, queue, state)
    IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, messages, new_state}
  end

  @impl true
  def handle_demand(incoming_demand, %{queue: queue, demand: demand} = state)
      when incoming_demand > 0 do
    IO.inspect(incoming_demand, label: "@@@ Incoming Demand")
    IO.inspect(demand, label: "@@@ Stored Demand")

    total_demand = incoming_demand + demand

    {messages, new_state} = fetch_and_release_demand(total_demand, queue, state)
    IO.inspect(new_state, label: "@@@ State returned")
    {:noreply, messages, new_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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
end
