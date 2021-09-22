defmodule CogyntWorkstationIngest.Elasticsearch.API do

  alias Elasticsearch.Index
  alias CogyntWorkstationIngest.Elasticsearch.Cluster
  alias CogyntWorkstationIngest.Config

  def index_exists?(index) do
    with {:ok, _} <- Index.latest_starting_with(Cluster, index) do
      true
    else
      {:error, _} ->
        false
    end
  end

  def create_index(index) do
    name = "#{index}_#{today_date()}"
    Elasticsearch.Index.create_from_file(CogyntWorkstationIngest.Elasticsearch.Cluster, name, "priv/elasticsearch/event.json")
    Elasticsearch.Index.alias(CogyntWorkstationIngest.Elasticsearch.Cluster, name, Config.event_index_alias())
  end

  def index_health?(index) do
    Elasticsearch.get(
      CogyntWorkstationIngest.Elasticsearch.Cluster,
      "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
    )
  end

  def reindex(index) do
    config = Elasticsearch.Cluster.Config.get(CogyntWorkstationIngest.Elasticsearch.Cluster)
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

  def bulk_upload(config, index, index_config) do
    case Elasticsearch.Index.Bulk.upload(config, index, index_config) do
      :ok ->
        :ok

      {:error, errors} -> errors
    end
  end

  def delete_on_query() do

  end

  def search_query() do
  end

  def bulk_upsert() do

  end

    @doc false
    defp today_date(), do: Timex.now() |> Timex.format!("%Y%m%d", :strftime)

end
