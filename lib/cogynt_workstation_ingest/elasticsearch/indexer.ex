defmodule CogyntWorkstationIngest.Elasticsearch.Indexer do
  @moduledoc """
  Genserver that takes care of the reindex requests.
  There is a single running instance of this process in the cluster.
  We pipe them all via a GenServer to not overload the ES server with too many requests.
  """
  use GenServer
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi
  alias __MODULE__.{IndexState, Starter}

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(args) do
    indices =
      Enum.reduce(Keyword.get(args, :aliases, []), %{}, fn a, acc ->
        ElasticApi.latest_starting_with(a)
        |> case do
          {:ok, index} ->
            Map.put(acc, a, %IndexState{index: index, alias: a})

          {:error, :not_found} ->
            Map.put(acc, a, %IndexState{alias: a})

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Could not determine latest index for alias #{a}"
            )

            Map.put(acc, a, %IndexState{alias: a, state: :error, error: error})
        end
      end)

    {:ok, args |> Enum.into(%{}) |> Map.put(:indices, indices), {:continue, :check_index}}
  end

  @impl GenServer
  def handle_continue(:check_index, %{indices: indices} = state) do
    CogyntLogger.info("#{__MODULE__}", "Creating Elastic Indices...")

    indices =
      Enum.reduce(indices, indices, fn
        {a, %{index: nil} = i}, acc ->
          IndexState.transit(i, :creating_index)
          |> case do
            {:ok, i} ->
              Process.send(Starter.whereis(__MODULE__), {:create_index, i}, [:noconnect])
              Map.put(acc, a, i)

            {:error, error_i} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Unable to transition index to a :creating_index. #{inspect(i)}"
              )

              Map.put(acc, a, error_i)
          end

        {a, i}, acc ->
          IndexState.transit(i, :checking_reindex)
          |> case do
            {:ok, i} ->
              Process.send(Starter.whereis(__MODULE__), {:check_to_reindex, i}, [:noconnect])
              Map.put(acc, a, i)

            {:error, error_i} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Unable to transition index to a :checking_reindex state. #{inspect(i)}"
              )

              Map.put(acc, a, error_i)
          end
      end)

    {:noreply, %{state | indices: indices}}
  end

  @impl GenServer
  def handle_info({:check_to_reindex, %{alias: index_alias} = i}, %{indices: indices} = state) do
    index =
      ElasticApi.check_to_reindex(index_alias)
      |> case do
        :ok ->
          CogyntLogger.info("#{__MODULE__}", "Reindexing Check complete..")
          IndexState.transit(i, :ready)

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Unable to transition index to a :checking_reindex state. #{inspect(i)}"
          )

          IndexState.transit(i, :error, error: error)
      end

    {:noreply, %{state | indices: Map.put(indices, index_alias, index)}}
  end

  @impl GenServer
  def handle_info({:create_index, %{alias: index_alias} = i}, %{indices: indices} = state) do
    index =
      ElasticApi.create_index(index_alias)
      |> case do
        {:ok, true} ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "The Index: #{index_alias} for CogyntWorkstation has been created."
          )

          IndexState.transit(i, :ready)

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Unable to transition index to a :checking_reindex state. #{inspect(i)}"
          )

          IndexState.transit(i, :error, error: error)
      end

    {:noreply, %{state | indices: Map.put(indices, index_alias, index)}}
  end
end
