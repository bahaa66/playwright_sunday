defmodule CogyntWorkstationIngest.Elasticsearch.IndexerStarter do
  # CURRENTLY NOT USED UNTIL WE START USING LIBCLUSTER AND HORDE REGISTRY AGAIN
  # NEED TO FIGURE OUT WHY THEY DONT WORK WITH ISTIO
  alias CogyntElasticsearch.Indexer
  alias CogyntWorkstationIngest.Horde.HordeRegistry
  alias CogyntWorkstationIngest.Elasticsearch.IndexerSupervisor

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :temporary,
      shutdown: 500
    }
  end

  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:name, Indexer)
      |> via_tuple()

    new_opts = Keyword.put(opts, :name, name)

    child_spec = %{
      id: Indexer,
      start: {Indexer, :start_link, [new_opts]}
    }

    IndexerSupervisor.start_child(child_spec)
    |> case do
      {:error, {:already_started, pid}} -> {:ok, pid}
      res -> res
    end
  end

  def whereis(name \\ Indexer) do
    name
    |> via_tuple()
    |> GenServer.whereis()
  end

  defp via_tuple(name) do
    {:via, Horde.Registry, {HordeRegistry, name}}
  end
end
