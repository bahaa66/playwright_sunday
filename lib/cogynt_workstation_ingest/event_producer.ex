defmodule CogyntWorkstationIngest.EventProducer do
  use GenStage
  require Logger
  alias KafkaEx.Protocol.Fetch.Message

  @message_set "message_set_key"

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(_args) do
    GenStage.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(broadway_args) do
    table_name = broadway_args[:table]
    key = broadway_args[:cache_key]

    {:ok, _table} = create_ets_table(table_name)

    Process.flag(:trap_exit, true)

    {
      :producer,
      %{
        message_list: message_list(table_name, key),
        cache_key: key,
        producer_name: broadway_args[:name],
        table_name: table_name
      }
    }
  end

  def populate_state(message_set) when is_list(message_set) do
    # TODO figure out how to call specific name of Broadway producer for callbacks
    GenServer.cast(
      :"Elixir.CogyntWorkstationIngest.EventPipelineTEST.Broadway.Producer_0",
      {:populate_state, message_set}
    )
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def handle_cast({:populate_state, message_set}, state) do
    IO.inspect(state, label: "@@@ STATE")

    events =
      Enum.reduce(message_set, [], fn json_message, acc ->
        case Jason.decode(json_message) do
          {:ok, message} ->
            acc ++ [message]

          {:error, error} ->
            Logger.error("Failed to decode json_message. Error: #{inspect(error)}")
        end
      end)

    IO.inspect(events, label: "@@@ Decoded events")
    update_table(state.cache_key, events)

    {:noreply, state}
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    IO.inspect(demand, label: "@@@ Demand from processors")
    IO.inspect(state.message_list, label: "@@@ Current message_list")

    {to_dispatch, remaining} = Enum.split(state.message_list, demand)

    IO.inspect(remaining, label: "@@@ Items remaining after #{Enum.count(to_dispatch)} dispatched")

    updated_state = Map.put(state, :message_list, remaining)

    {:noreply, to_dispatch, updated_state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  # creates an ets table from data in the file system if it exists
  # if not it generates a new empty table
  defp create_ets_table(table_name) do
    file_path = String.to_charlist(file_path() <> Atom.to_string(table_name) <> ".tab")

    case :ets.file2tab(file_path) do
      {:ok, table} ->
        File.rm(file_path)
        {:ok, table}

      {:error, _reason} ->
        table = :ets.new(table_name, [:named_table, :set])
        {:ok, table}
    end
  end

  defp update_table(key, value) do
    :ets.insert(table_name, {key, value})
  end

  defp message_list(table_name, key) do
    case :ets.lookup(table_name, key) do
      [] ->
        []

      [{_key, message_set}] ->
        message_set
    end
  end

  defp file_path(), do: "/tmp/"
end
