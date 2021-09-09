defmodule CogyntWorkstationIngest.Elasticsearch.Store do
  @behaviour Elasticsearch.Store

  import Ecto.Query
  alias CogyntWorkstationIngest.Repo

  @impl true
  def stream(schema) do
    IO.puts("***********in schema************")
    schema |> IO.inspect()
    Repo.stream(schema)
  end

  @impl true
  def transaction(fun) do
    IO.puts("***********in transaction************")
    {:ok, result} = Repo.transaction(fun, timeout: :infinity)
  end
end
