defmodule CogyntWorkstationIngest.Elasticsearch.Store do
  @behaviour Elasticsearch.Store

  import Ecto.Query
  alias CogyntWorkstationIngest.Repo

  @impl true
  def stream(schema) do
    schema
    |> Repo.stream()
    |> Stream.chunk_every(500) #add a chunk size
    |> Stream.flat_map(fn chunk -> Repo.preload(chunk, [event_definition: [:event_definition_details]])
end)
  end

  @impl true
  def transaction(fun) do
    {:ok, result} = Repo.transaction(fun, timeout: 60_000)
  end
end
