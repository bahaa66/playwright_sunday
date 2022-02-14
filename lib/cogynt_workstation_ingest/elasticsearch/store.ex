defmodule CogyntWorkstationIngest.Elasticsearch.Store do
  @behaviour Elasticsearch.Store

  alias CogyntWorkstationIngest.Repo

  @impl true
  def stream(schema) do
    schema
    |> Repo.stream()
    # add a chunk size
    |> Stream.chunk_every(10)
    |> Stream.flat_map(fn chunk ->
      Repo.preload(chunk, event_definition: [:event_definition_details])
    end)
  end

  @impl true
  def transaction(fun) do
    # 3 minutes works for 2.43 million records in es
    {:ok, _result} = Repo.transaction(fun, timeout: :infinity)
  end
end
