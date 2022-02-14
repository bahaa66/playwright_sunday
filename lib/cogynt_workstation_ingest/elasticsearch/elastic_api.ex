defmodule CogyntWorkstationIngest.Elasticsearch.ElasticApi do
  alias Elasticsearch.Index
  alias CogyntWorkstationIngest.Elasticsearch.Cluster
  alias CogyntWorkstationIngest.Config

  # --------------------- #
  # --- Index Methods --- #
  # ---------------------- #
  def check_to_reindex() do
    case is_active_index_setting?() do
      true ->
        CogyntLogger.info("#{__MODULE__}", "check_to_reindex Event Index already exists...")

      false ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "check_to_reindex Event index mapping is out dated. Triggering Reindex..."
        )

        reindex(Config.event_index_alias())
    end
  end

  def index_exists?(index) do
    try do
      with {:ok, _} <- latest_starting_with(index) do
        {:ok, true}
      else
        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "index_exists?/1 Failed to check for index: #{index}. Error: #{inspect(error)}"
          )

          {:ok, false}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "index_exists?/1 Unable to connect to Elasticsearch while checking if index_exists. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, e.reason}
    end
  end

  def create_index(index) do
    name = build_name(index)
    settings_file = index_mappings_file()

    try do
      case Elasticsearch.Index.create_from_file(Cluster, name, settings_file) do
        :ok ->
          Index.alias(Cluster, name, Config.event_index_alias())

          CogyntLogger.info(
            "#{__MODULE__}",
            "create_index/1 Success. Index: #{name}"
          )

          {:ok, true}

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "create_index/1 Failed to create index: #{index}. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      e in HTTPoison.Error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "create_index/1 Unable to connect to Elasticsearch while creating index alias. Index: #{index} Error: #{inspect(e.reason)}"
        )

        {:error, e.reason}
    end
  end

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

  @doc """
  Returns all indexes which start with a given string.
  ## Example
      iex> Index.create_from_file("posts_1", "test/support/settings/posts.json")
      ...> Index.starting_with("posts")
      {:ok, ["posts_1"]}
  """
  def starting_with(prefix) do
    with {:ok, indexes} <- Elasticsearch.get(Cluster, "/_cat/indices?format=json") do
      prefix = prefix |> to_string() |> Regex.escape()
      {:ok, regex} = Regex.compile("^#{prefix}_[0-9]+$")

      indexes =
        indexes
        |> Enum.map(& &1["index"])
        |> Enum.filter(&Regex.match?(regex, &1))
        |> Enum.sort()

      {:ok, indexes}
    else
      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "starting_with/1 Failed to get indices from Elasticsearch #{inspect(error)}"
        )

        {:error, error}
    end
  end

  @doc """
  Gets the most recent index name with the given prefix.
  ## Examples
      iex> create_from_file("posts_1", "test/support/settings/posts.json")
      ...> create_from_file("posts_2", "test/support/settings/posts.json")
      ...> latest_starting_with("posts")
      {:ok, "posts-2"}
  If there are no indexes matching that prefix:
      iex> latest_starting_with("nonexistent")
      {:error, :not_found}
  """
  def latest_starting_with(prefix) do
    with {:ok, indexes} <- starting_with(prefix) do
      index =
        indexes
        |> List.last()

      case index do
        nil ->
          {:error, :not_found}

        index ->
          {:ok, index}
      end
    end
  end

  def reindex(index) do
    config = Elasticsearch.Cluster.Config.get(Cluster)
    index_alias = String.to_atom(index)
    name = build_name(index_alias)
    index_config = config[:indexes][index_alias]
    settings_file = index_mappings_file()

    with :ok <- Elasticsearch.Index.create_from_file(config, name, settings_file),
         bulk_upload(config, name, index_config),
         :ok <- Elasticsearch.Index.alias(config, name, index_alias),
         :ok <- clean_starting_with(config, index_alias, 1),
         :ok <- Elasticsearch.Index.refresh(config, name) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "reindex/1 The Event index #{name} has been reindexed..."
      )

      :ok
    else
      errors ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "reindex/1 Failed. Error: #{inspect(errors)}"
        )

      {:error, errors} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "reindex/1 Failed. Error: #{inspect(errors)}"
        )
    end
  end

  # ------------------------ #
  # --- Document Methods --- #
  # ------------------------ #

  def bulk_upload(config, index, index_config) do
    case Elasticsearch.Index.Bulk.upload(config, index, index_config) do
      :ok ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "bulk_upload/3 complete for Elasticsearch index: #{index}"
        )

        :ok

      {:error, errors} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "bulk_upload/3 Failed for Elasticsearch index: #{index}. Error: #{inspect(errors)}"
        )

        errors
    end
  end

  def bulk_upsert_document(index, bulk_docs) do
    encoded_data =
      bulk_docs
      |> Enum.map(&encode!(&1, index))
      |> Enum.join("\n")

    try do
      case Elasticsearch.post(
             CogyntWorkstationIngest.Elasticsearch.Cluster,
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
          # TODO: ???
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
          CogyntLogger.info(
            "#{__MODULE__}",
            "bulk_delete/2 Success. Result: #{inspect(result, pretty: true)}"
          )

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

  defp is_active_index_setting?() do
    settings_file = index_mappings_file()

    with {:ok, body} <- File.read(settings_file),
         {:ok, settings} <- get_index_mappings(),
         {:ok, json} <- Jason.decode(body) do
      Map.equal?(Map.get(json, "settings"), Map.get(settings, "settings")) and
        Map.equal?(Map.get(json, "mappings"), Map.get(settings, "mappings"))
    else
      {:error, reason} ->
        IO.puts("Cannot read file because #{reason}")
        false
    end
  end

  defp get_index_mappings() do
    with {:ok, index} <- latest_starting_with(Config.event_index_alias()),
         {:ok, %{^index => %{"settings" => settings}}} <-
           Elasticsearch.get(Cluster, "#{Config.event_index_alias()}/_settings"),
         {:ok, %{^index => mappings}} <-
           Elasticsearch.get(Cluster, "#{Config.event_index_alias()}/_mapping") do
      index =
        settings
        |> Map.get("index")
        |> Map.drop(["creation_date", "provided_name", "uuid", "version"])

      {:ok, Map.merge(%{"settings" => %{"index" => index}}, mappings)}
    else
      {:error, reason} ->
        IO.puts("Cannot get Elasticsearch Index Settings or Mappings because " <> reason)
        {:error, reason}
    end
  end

  # Generates a name for an index that will be aliased to a given `alias`.
  # Similar to migrations, the name will contain a timestamp.
  # ## Example
  #     Index.build_name("main")
  #     # => "main-1509581256"

  defp build_name(alias) do
    "#{alias}_#{system_timestamp()}"
  end

  # Removes indexes starting with the given prefix, keeping a certain number.
  # Can be used to garbage collect old indexes that are no longer used.
  #  ## Examples
  #  If there is only one index, and `num_to_keep` is >= 1, the index is not deleted.
  #  iex> Index.create_from_file(Cluster, "posts-1", "test/support/settings/posts.json")
  #  ...> Index.clean_starting_with(Cluster, "posts", 1)
  #  ...> Index.starting_with(Cluster, "posts")
  #  {:ok, ["posts-1"]}
  #   If `num_to_keep` is less than the number of indexes, the older indexes are
  #   deleted.
  #  iex> Index.create_from_file(Cluster, "posts-1", "test/support/settings/posts.json")
  #  ...> Index.clean_starting_with(Cluster, "posts", 0)
  #  ...> Index.starting_with(Cluster, "posts")
  #  {:ok, []}
  defp clean_starting_with(cluster, prefix, num_to_keep) when is_integer(num_to_keep) do
    with {:ok, indexes} <- starting_with(prefix) do
      total = length(indexes)
      num_to_delete = total - num_to_keep
      num_to_delete = if num_to_delete >= 0, do: num_to_delete, else: 0

      errors =
        indexes
        |> Enum.sort()
        |> Enum.take(num_to_delete)
        |> Enum.map(&Elasticsearch.delete(cluster, "/#{&1}"))
        |> Enum.filter(&(elem(&1, 0) == :error))
        |> Enum.map(&elem(&1, 1))

      if length(errors) > 0 do
        {:error, errors}
      else
        :ok
      end
    end
  end

  defp system_timestamp do
    # 2021-10-20 18:37:13Z --> "20211020183713"
    DateTime.utc_now()
    |> DateTime.truncate(:second)
    |> DateTime.to_string()
    |> String.replace(["-", ":", " ", "Z"], "")
  end

  defp index_mappings_file() do
    priv_folder = Application.app_dir(:cogynt_workstation_ingest, "priv/elasticsearch")

    if Config.env() == :prod do
      Path.join(priv_folder, "event.prod.active.json")
    else
      Path.join(priv_folder, "event.dev.active.json")
    end
  end
end
