defmodule CogyntWorkstationIngest.Elasticsearch.Store do
  @behaviour Elasticsearch.Store

  import Ecto.Query
  alias CogyntWorkstationIngest.Repo

  @impl true
  def stream(schema) do
    IO.puts("*************IN STREAM JUST STREAM**************")
    schema
    |> Repo.stream()
    |> Stream.chunk_every(10)
    |> Stream.flat_map(fn chunk -> Repo.preload(chunk, [event_definition: [:event_definition_details]])
end)
  end

  @impl true
  def transaction(fun) do
    IO.puts("***********IN TRANSACTION*******************")
    {:ok, result} = Repo.transaction(fun, timeout: :infinity)
  end
end
