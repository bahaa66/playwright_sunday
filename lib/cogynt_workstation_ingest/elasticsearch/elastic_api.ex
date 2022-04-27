defmodule CogyntWorkstationIngest.Elasticsearch.ElasticApi do
  alias Elasticsearch.Index
  alias CogyntWorkstationIngest.Elasticsearch.Cluster

  # --------------------- #
  # --- Index Methods --- #
  # ---------------------- #
  def index_health?(index) do
    try do
      case Elasticsearch.get(
             Cluster,
             "_cluster/health/#{index}?wait_for_status=green&timeout=10s"
           ) do
        {:ok, _result} ->
          {:ok, true}

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to return index health, index: #{index}. Error: #{inspect(error)}"
          )

          {:error, false}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to connect to Elasticsearch while checking index health. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, false}
    end
  end

  # ------------------------ #
  # --- Document Methods --- #
  # ------------------------ #

  def bulk_upsert_document(index, bulk_docs) do
    encoded_data =
      bulk_docs
      |> Enum.map(&encode!(&1, index))
      |> Enum.join("\n")

    try do
      case Elasticsearch.post(
             Cluster,
             "_bulk",
             encoded_data
           ) do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "bulk_upsert_document/2 Failed to bulk upsert documents for index: #{index}. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "bulk_upsert_document/2 Unable to connect to Elasticsearch while bulk upserting document. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, e.reason}
    end
  end

  def delete_by_query(index, query_data) do
    query = build_term_query(query_data)

    url =
      url(
        index,
        "_delete_by_query?refresh=true&slices=auto&scroll_size=10000"
      )

    try do
      case Elasticsearch.post(Cluster, url, query) do
        {:ok, %{"deleted" => deleted}} ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "delete_by_query/2 removed #{deleted} documents from Elasticsearch"
          )

          {:ok, deleted}

        {:ok, _} ->
          {:ok, 0}

        {:error, reason} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "delete_by_query/2 failed to remove data from Elasticsearch. Reason: #{inspect(reason)}"
          )

          {:error, reason}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "delete_by_query/2 Unable to connect to elasticsearch for delete_by_query. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, e.reason}
    end
  end

  def bulk_delete(index, bulk_delete_ids) do
    bulk_delete_data = prepare_bulk_delete_data(index, bulk_delete_ids)

    try do
      case Elasticsearch.post(Cluster, "_bulk", bulk_delete_data) do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "bulk_delete/2 Failed to bulk delete documents for index: #{index}. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "bulk_delete/2 Unable to connect to Elasticsearch while bulk deleting document. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, e.reason}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp encode!(struct, index, action \\ "update") do
    header = header(action, index, struct)

    document = Jason.encode!(%{"doc" => struct, "doc_as_upsert" => true})

    "#{header}\n#{document}\n"
  end

  defp header(type, index, struct) do
    attrs = %{
      "_index" => index,
      "_id" => struct[:id],
      "retry_on_conflict" => 5
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
