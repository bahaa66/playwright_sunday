defmodule CogyntWorkstationIngest.Elasticsearch.API do

  alias Elasticsearch.Index
  alias CogyntWorkstationIngest.Elasticsearch.Cluster
  alias CogyntWorkstationIngest.Config


  def index_exists?(index) do
    try do
      with {:ok, _} <- Index.latest_starting_with(Cluster, index) do
        {:ok, true}
      else
        {:error, _} ->
          {:ok, false}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "Elasticsearch Connection Failure",
          "Unable to connect to elasticsearch while checking if index_exists. Index: #{index} Error: #{
            e.reason
          }"
        )

        {:error, e.reason}
    end
  end

  def create_index(index) do
    name = Index.build_name(index)
    #TBD get configs
    try do
      case Elasticsearch.Index.create_from_file(Cluster, name, "priv/elasticsearch/event.active.json") do
        :ok ->
          Index.alias(Cluster, name, Config.event_index_alias())
          {:ok, true}

          {:error, error} ->
            CogyntLogger.error(
              "Creating Elasticsearch Index Error",
              "Failed to create index: #{index}. Error: #{inspect(error)}"
            )
            {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "Elasticsearch Connection Failure",
          "Unable to connect to elasticsearch while creating index alias. Index: #{index} Error: #{
            e.reason
          }"
        )

        {:error, e.reason}
    end
  end

  def index_health(index) do
    try do
      case Elasticsearch.get(Cluster,
        "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
      ) do
        {:ok, _result} ->
          {:ok, true}

        {:error, error} ->
          CogyntLogger.error(
            "Checking Elasticsearch Index Health Error",
            "Failed to return index health, index: #{index}. Error: #{inspect(error)}"
          )

          {:error, false}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "Elasticsearch Connection Failure",
          "Unable to connect to elasticsearch while checking index health. Index: #{index} Error: #{
            e.reason
          }"
        )

        {:error, false}
    end
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

  def delete_by_query(index, query_data) do

    query = build_term_query(query_data)

    url = url(
      index,
    "_delete_by_query?refresh=true&slices=auto&scroll_size=10000&requests_per_second=100"
    )

    try do
      case Elasticsearch.post(Cluster, url, query)
            do
              {:ok, result} ->
                deleted = Map.get(result, "deleted")

                CogyntLogger.info(
                  "Removed Record From Elastic",
                  "delete_by_query removed #{deleted} records from Elasticsearch"
                )

                {:ok, deleted}

              {:error, reason} ->
                CogyntLogger.error(
                  "Failed To Remove Record From Elasticsearch",
                  "delete_by_query failed with reason #{inspect(reason)}"
                )

                {:error, reason}
            end
          rescue
            e in HTTPoison.Error ->
              CogyntLogger.error(
                "Elasticsearch Connection Failure",
                "Unable to connect to elasticsearch for delete_by_query. Index: #{index} Error: #{
                  e.reason
                }"
              )

              {:error, e.reason}
          end
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

    try do
      case Elasticsearch.post(Cluster, "_bulk", bulk_delete_data) do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          CogyntLogger.error(
            "Elasticsearch Bulk Delete Error",
            "Failed to bulk delete documents for index: #{index}. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "Elasticsearch Connection Failure",
          "Unable to connect to elasticsearch while bulk deleting document. Index: #{index} Error: #{
            e.reason
          }"
        )

        {:error, e.reason}
    end
  end


    # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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

  defp url(index, action) when is_binary(action),
  do: "/#{index}/#{action}"


  defp build_term_query(%{field: field, value: value}) do
    %{
      query: %{
        term: %{
          "#{field}.keyword" => %{
            value: "#{value}"
          }
        }
      }
    }
  end
end
