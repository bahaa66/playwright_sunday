defmodule CogyntWorkstationIngest.Elasticsearch.API do
  def index_exists(index) do
    with {:ok, _index} <- latest_index_starting_with("event_test") do
      true
    else
      {:error, _} ->
        false
    end
  end

  def index_starting_with(prefix) do
    with {:ok, indexes} <-
           Elasticsearch.get(
             CogyntWorkstationIngest.Elasticsearch.Cluster,
             "/_cat/indices?format=json"
           ) do
      prefix = prefix |> to_string() |> Regex.escape()
      {:ok, prefix} = Regex.compile("^#{prefix}-[0-9]+$")

      indexes =
        indexes
        |> Enum.map(& &1["index"])
        |> Enum.filter(&String.match?(&1, prefix))
        |> Enum.sort()

      {:ok, indexes}
    end
  end

  def latest_index_starting_with(prefix) do
    with {:ok, indexes} <- index_starting_with(prefix) do
      index =
        indexes
        |> Enum.sort()
        |> List.last()

      case index do
        nil -> {:error, :not_found}
        index -> {:ok, index}
      end
    end
  end

  def index_health(index) do
    Elasticsearch.get(
      CogyntWorkstationIngest.Elasticsearch.Cluster,
      "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
    )
  end
end
