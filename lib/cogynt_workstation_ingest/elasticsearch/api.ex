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
    name = Index.build_name(index)
    case Elasticsearch.Index.create_from_file(Cluster, name, "priv/elasticsearch/event.json") do
      :ok ->
        Index.alias(Cluster, name, Config.event_index_alias())
        :ok

        {:error, error} ->
          CogyntLogger.error(
            "Creating Elasticsearch Index Error",
            "Failed to create index: #{index}. Error: #{inspect(error)}"
          )
    end

  end

  def index_health(index) do
   Elasticsearch.get(Cluster,
      "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
    )
  end

  def reindex(index) do
    config = Elasticsearch.Cluster.Config.get(Cluster)
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

  def delete_by_query() do

  end

  def search_query() do
  end

  def bulk_upsert_document(index, bulk_docs) do
    {:ok, index} = Elasticsearch.Index.latest_starting_with(Cluster, index) |> IO.inspect()

    encoded_data = bulk_docs
    |> Enum.map(&encode!(&1, index))
    |> Enum.join("\n")

    try do
      case Elasticsearch.post(CogyntWorkstationIngest.Elasticsearch.Cluster, "_bulk", encoded_data) do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          CogyntLogger.error(
            "Elasticsearch Bulk Upsert Error",
            "Failed to bulk upsert documents for index: #{index}. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "Elasticsearch Connection Failure",
          "Unable to connect to elasticsearch while bulk upserting document. Index: #{index} Error: #{
            e.reason
          }"
        )

        {:error, e.reason}
    end
  end

  def bulk_delete(index, bulk_delete_ids) do
    {:ok, index} = Elasticsearch.Index.latest_starting_with(Cluster, index) |> IO.inspect()

    bulk_delete_data = prepare_bulk_delete_data(index, bulk_delete_ids)

    Elasticsearch.post(Cluster, "_bulk", bulk_delete_data)
  end


    # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp valid_core_id?(core_id) do
    case is_nil(core_id) do
      true ->
        true

      false ->
        is_binary(core_id)
    end
  end

  defp encode!(struct, index, action \\ "create") do
    header = header(action, index, struct)

    document = Jason.encode!(struct)

    "#{header}\n#{document}\n"
  end

  defp header(type, index, struct) do
    attrs = %{
      "_index" => index,
      "_id" => struct[:id]
    }
    Jason.encode!(%{type => attrs})
  end

  @doc false
  defp prepare_bulk_delete_data(index, ids) do
    Enum.map(ids, fn id ->
      %{delete: %{_index: index, _id: id}}
    end)
    |> format_bulk_data()
  end

  @doc false
  defp format_bulk_data(bulk_data) do
    bulk_data =
      Enum.map(bulk_data, fn data -> Jason.encode!(data) end)
      |> Enum.join("\n")

    bulk_data <> "\n"
  end

end
