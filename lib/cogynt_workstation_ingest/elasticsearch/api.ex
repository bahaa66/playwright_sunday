defmodule CogyntWorkstationIngest.Elasticsearch.API do

  def index_exists?(index) do
    with {:ok, _} <- latest_index_starting_with(index) do
      true
    else
      {:error, _} ->
        false
    end
  end

  def create_index(index) do
    Elasticsearch.Index.create_from_file(CogyntWorkstationIngest.Elasticsearch.Cluster, "event_test", "priv/elasticsearch/event.json")
    Elasticsearch.Index.alias(CogyntWorkstationIngest.Elasticsearch.Cluster, "event_test", "event_test")
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

  def index_health?(index) do
    Elasticsearch.get(
      CogyntWorkstationIngest.Elasticsearch.Cluster,
      "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
    )
  end

  def reindex(index) do
    config = Elasticsearch.Cluster.Config.get(CogyntWorkstationIngest.Elasticsearch.Cluster) |> IO.inspect()
    alias = String.to_existing_atom(index)
    name = Elasticsearch.Index.build_name(alias)
    %{settings: settings_file} = index_config = config[:indexes][alias]

    with :ok <- Elasticsearch.Index.create_from_file(config, name, settings_file),
         bulk_upload(config, name, index_config),
         :ok <- Elasticsearch.Index.alias(config, name, alias),
         :ok <- Elasticsearch.Index.clean_starting_with(config, alias, 2),
         :ok <- Elasticsearch.Index.refresh(config, name) do
          :ok
         end
  end

  def bulk_upload(config, name, index_config) do
    case Elasticsearch.Index.Bulk.upload(config, name, index_config) do
      :ok ->
        :ok

      {:error, errors} ->
        IO.puts(errors)
        errors
    end
  end

  def delete_on_query() do

  end

  def search_query() do
  end

end
