defmodule CogyntWorkstationIngest.ElasticsearchAPI do

  alias Elasticsearch.Index
  alias CogyntWorkstationIngest.Elasticsearch.Cluster
  alias CogyntWorkstationIngest.Config


  def index_exists?(index) do
    try do
      with {:ok, _} <- latest_starting_with(index) do
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
    name = build_name(index)
    priv_folder = Application.app_dir(:cogynt_workstation_ingest, "priv/elasticsearch")
    settings_file = if Config.env() == :dev do
      Path.join(priv_folder, "event.dev.active.json")
    else
      Path.join(priv_folder, "event.prod.active.json")
    end

    IO.puts("Settings file path #{settings_file}")
    try do
      case Elasticsearch.Index.create_from_file(Cluster, name, settings_file) do
        :ok ->
          Index.alias(Cluster, name, Config.event_index_alias())
          IO.puts("Created index: #{name}")
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

  def index_health?(index) do
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

  @doc """
  Returns all indexes which start with a given string.
  ## Example
      iex> Index.create_from_file("posts_1", "test/support/settings/posts.json")
      ...> Index.starting_with("posts")
      {:ok, ["posts_1"]}
  """
  @spec starting_with( String.t() | atom) ::
          {:ok, [String.t()]}
          | {:error, Elasticsearch.Exception.t()}
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
  @spec latest_starting_with(String.t() | atom) ::
          {:ok, String.t()}
          | {:error, :not_found}
          | {:error, Elasticsearch.Exception.t()}
  def latest_starting_with(prefix) do
    with {:ok, indexes} <- starting_with(prefix) do
      index =
        indexes
        |> Enum.sort()
        |> List.last()

      case index do
        nil -> {:error, :not_found}
        index -> IO.puts("The latest index is #{index}")
        {:ok, index}
      end
    end
  end

  def reindex(index) do
    config = Elasticsearch.Cluster.Config.get(Cluster)
    alias = String.to_existing_atom(index)
    name = build_name(alias)
    index_config = config[:indexes][alias]
    priv_folder = Application.app_dir(:cogynt_workstation_ingest, "priv/elasticsearch")
    settings_file = if Config.env() == :dev do
      Path.join(priv_folder, "event.dev.active.json")
    else
      Path.join(priv_folder, "event.prod.active.json")
    end

    with :ok <- Elasticsearch.Index.create_from_file(config, name, settings_file),
         bulk_upload(config, name, index_config),
         :ok <- Elasticsearch.Index.alias(config, name, alias),
         :ok <- clean_starting_with(config, alias, 1),
         :ok <- Elasticsearch.Index.refresh(config, name) do
          IO.puts("The event index #{name} for CogyntWorkstation have been created by reindexing.....")
          :ok
         else
          errors ->
            IO.puts("Error while Reindexing #{inspect errors}")
          {:error, errors} ->
            IO.puts("Error while Reindexing #{inspect errors}")
         end
  end

  def bulk_upload(config, index, index_config) do
    case Elasticsearch.Index.Bulk.upload(config, index, index_config) do
      :ok ->
        IO.puts("Bulk upload complete for index #{index}")
        :ok

      {:error, errors} ->
        IO.puts("Error while bulk uploading #{inspect errors}")
        errors
    end
  end

  def delete_by_query(index, query_data) do

    query = build_term_query(query_data)

    url = url(
      index,
    "_delete_by_query?refresh=true&slices=auto&scroll_size=10000"
    )

    try do
      case Elasticsearch.post(Cluster, url, query) do

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

  def bulk_upsert_document(index, bulk_docs) do
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

  def check_to_reindex() do
    case is_active_index_setting?() do
      true ->
        IO.puts("event_index already exists.")

      false ->
        IO.puts("Current Index mapping is not current....")
        reindex(Config.event_index_alias())

      {:error, error} ->
          CogyntLogger.error(
            "Elasticsearch Check to Reindex",
            "Failed to read settings/mappings. #{inspect(error)}"
          )

          {:error, error}
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

  defp is_active_index_setting?() do
    priv_folder = Application.app_dir(:cogynt_workstation_ingest, "priv/elasticsearch")

    settings_file = if Config.env() == :dev do
      Path.join(priv_folder, "event.dev.active.json")
    else
      Path.join(priv_folder, "event.prod.active.json")
    end

    with {:ok, body} <- File.read(settings_file),
    {:ok, settings} <- get_index_mappings(),
      {:ok, json} <- Poison.decode(body)  do
        #compare settings, mappings separately as comparing json |> Map.equal?(settings) returns false as its compared using ===
          settings_equal? =   Map.equal?(Map.get(json, "settings"), Map.get(settings, "settings"))
          mappings_equal? = Map.equal?(Map.get(json, "mappings"), Map.get(settings, "mappings"))
          IO.puts("Settings are equal? #{settings_equal?}")
          IO.puts("Mappings are equal? #{mappings_equal?}")
          #if either one is not equal return false.
          settings_equal? && mappings_equal?
    else
      {:error, reason} ->
        IO.puts("Cannot read file because #{reason}")
        {:error, reason}
    end
  end

  defp get_index_mappings() do
    with {:ok, index} <- latest_starting_with(Config.event_index_alias()),
    {:ok, %{ ^index => %{"settings" => settings }} } <- Elasticsearch.get(Cluster, "#{Config.event_index_alias}/_settings"),
    {:ok, %{^index => mappings} } <- Elasticsearch.get(Cluster, "#{Config.event_index_alias}/_mapping") do
      index = settings |> Map.get("index") |> Map.drop(["creation_date", "provided_name", "uuid", "version"])
      {:ok, Map.merge(%{"settings" => %{"index" => index }}, mappings)}
    else
      {:error, reason} ->
        IO.puts("Cannot get Elasticsearch Index Settings or Mappings because " <> reason)
        {:error, reason}
    end
  end

  @doc """
  Generates a name for an index that will be aliased to a given `alias`.
  Similar to migrations, the name will contain a timestamp.
  ## Example
      Index.build_name("main")
      # => "main-1509581256"
  """
  @spec build_name(String.t() | atom) :: String.t()
  def build_name(alias) do
    "#{alias}_#{system_timestamp()}"
  end


  @doc """
  Removes indexes starting with the given prefix, keeping a certain number.
  Can be used to garbage collect old indexes that are no longer used.
   ## Examples
   If there is only one index, and `num_to_keep` is >= 1, the index is not deleted.
   iex> Index.create_from_file(Cluster, "posts-1", "test/support/settings/posts.json")
   ...> Index.clean_starting_with(Cluster, "posts", 1)
   ...> Index.starting_with(Cluster, "posts")
   {:ok, ["posts-1"]}
    If `num_to_keep` is less than the number of indexes, the older indexes are
    deleted.
   iex> Index.create_from_file(Cluster, "posts-1", "test/support/settings/posts.json")
   ...> Index.clean_starting_with(Cluster, "posts", 0)
   ...> Index.starting_with(Cluster, "posts")
   {:ok, []}
  """
@spec clean_starting_with(Cluster.t(), String.t(), integer) ::
       :ok
       | {:error, [Elasticsearch.Exception.t()]}
def clean_starting_with(cluster, prefix, num_to_keep) when is_integer(num_to_keep) do
  with {:ok, indexes} <- starting_with( prefix) do
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
    DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_string() |> String.replace(["-", ":", " ", "Z"], "")
  end

end
