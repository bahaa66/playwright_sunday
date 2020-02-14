defmodule CogyntWorkstationIngest.Elasticsearch.EventDocument do
  @moduledoc """
  Document module for the Events index in elasticsearch
  """

  require Logger

  @initial_index_settings %{
    settings: %{
      index: %{
        max_result_window: 1_000_000,
        number_of_shards: 1,
        number_of_replicas: 0,
        refresh_interval: "1s"
      }
    },
    mappings: %{
      dynamic_templates: [
        %{
          dates_as_strings: %{
            match_mapping_type: "date",
            match: "field_*",
            mapping: %{
              type: "text",
              fields: %{
                raw: %{
                  type: "keyword",
                  ignore_above: 256
                }
              }
            }
          }
        }
      ]
    }
  }

  # @elasticsearch_client Application.get_env(:elasticsearch, :config)[:elasticsearch_client]

  @doc """
  Creates the index under the alias: events. With mapping: @initial_index_mapping
  """
  def create_index() do
    timestamp = today_date()
    timestamped_index = "#{index_alias()}_#{timestamp}"

    index_settings =
      Map.put_new(@initial_index_settings, :aliases, %{String.to_atom(index_alias()) => %{}})

    if elasticsearch_enabled?() do
      case Elasticsearch.create_index(timestamped_index, index_settings) do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          Logger.error(
            "Creating Elastic Index Error: Failed to create index: #{index_alias()}. Error: #{
              inspect(error)
            }"
          )

          {:error, error}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Deletes the index under the alias: events
  """
  def delete_index() do
    if elasticsearch_enabled?() do
      case Elasticsearch.get_index(index_alias()) do
        {:ok, result} ->
          [timestamped_index] = Map.keys(result)

          case Elasticsearch.delete_index(timestamped_index) do
            {:ok, result} ->
              {:ok, result}

            {:error, error} ->
              Logger.error(
                "Deleting Elastic Index Error: Failed to delete index #{index_alias()}. Error: #{
                  inspect(error)
                }"
              )

              {:error, error}
          end

        {:error, error} ->
          Logger.error(
            "Elastic Index Not Found: Failed to get index: #{index_alias()}. Error: #{
              inspect(error)
            }"
          )

          {:error, error}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Returns the value for the index_alias that the document is using
  """
  def get_index() do
    index_alias()
  end

  @doc """
  Returns true/false based on if the index under the alias: events exists.
  """
  def index_exists?() do
    if elasticsearch_enabled?() do
      case Elasticsearch.index_exists?(index_alias()) do
        {true, _index} ->
          {:ok, true}

        {false, _index} ->
          {:ok, false}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  If a document exists for the document_id it will update the existing document
  with the new document. If one is not found then it will create a new document
  under a new document_id.
  """
  def upsert_document(document, document_id) do
    index = index_alias()

    if elasticsearch_enabled?() do
      case document_exists?(document_id) do
        {:ok, true} ->
          case Elasticsearch.update_document(index, document_id, document) do
            {:ok, result} ->
              {:ok, convert_keys_to_atoms(result)}

            {:error, error} ->
              Logger.error(
                "Elastic Update Failed: Failed to update document for index: #{index}. Error: #{
                  inspect(error)
                }"
              )

              {:error, error}
          end

        {:error, false} ->
          case Elasticsearch.create_document(index, document_id, document) do
            {:ok, _result} ->
              {:ok, new_doc} = Elasticsearch.get_document(index, document_id)
              {:ok, convert_keys_to_atoms(new_doc)}

            {:error, error} ->
              Logger.error(
                "Elastic Create Document Error: Failed to create document for index: #{index}. Error: #{
                  inspect(error)
                }"
              )

              {:error, error}
          end

        {:ok, :elasticsearch_not_enabled} ->
          {:ok, :elasticsearch_not_enabled}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  The same implementation as upsert_document(_,_) but with a list of documents
  """
  def bulk_upsert_document(document_data) do
    if elasticsearch_enabled?() do
      if document_data != nil and Enum.empty?(document_data) != true do
        case Elasticsearch.bulk_upsert_document(index_alias(), document_data) do
          {:ok, _result} ->
            {:ok, :success}

          {:error, error} ->
            Logger.error(
              "Elastic Bulk Upsert Error: Failed to bulk upsert documents for index: #{
                index_alias()
              }. Error: #{inspect(error)}"
            )

            {:error, error}
        end
      else
        Logger.error(
          "Elastic Bulk Upsert Error: Failed to bulk upsert documents for index: #{index_alias()}. Error: passed in empty data set"
        )

        {:error, "passed in empty data set"}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Deletes a document based on its document_id
  """
  def delete_document(document_id) do
    if elasticsearch_enabled?() do
      case Elasticsearch.delete_document(index_alias(), document_id) do
        {:ok, _result} ->
          {:ok, document_id}

        {:error, error} ->
          Logger.error(
            "Elastic Delete Document Error: Failed to delete document for index: #{index_alias()}. Reason: #{
              inspect(error)
            }"
          )

          {:error, error}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Same as delete_document(_) but for a list of document_ids
  """
  def bulk_delete_document(document_ids) do
    if elasticsearch_enabled?() do
      case Elasticsearch.bulk_delete_document(index_alias(), document_ids) do
        {:ok, _result} ->
          {:ok, :success}

        {:error, error} ->
          Logger.error(
            "Elastic Bulk Delete Error: Failed to bulk delete documents for index: #{
              index_alias()
            }. Error: #{inspect(error)}"
          )

          {:error, error}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Deletes a document based on the results of a query ran against the index
  """
  def delete_by_query(args) do
    if elasticsearch_enabled?() do
      query_data = build_term_query(args)

      case Elasticsearch.delete_by_query(index_alias(), query_data) do
        {:ok, result} ->
          deleted = Map.get(result, "deleted")

          Logger.info(
            "Removed Record From Elastic: Delete_by_query removed #{deleted} records from ElasticSearch"
          )

          {:ok, deleted}

        {:error, reason} ->
          Logger.error(
            "Failed To Remove Record From Elastic: Delete_by_query failed with reason #{
              inspect(reason)
            }"
          )

          {:error, reason}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Returns true/false based on if the document exists for the document_id
  """
  def document_exists?(document_id) do
    if elasticsearch_enabled?() do
      case Elasticsearch.get_document(index_alias(), document_id) do
        {:ok, _source} ->
          {:ok, true}

        {:error, error} ->
          Logger.warn(
            "Elastic Document Not Found: Failed to look up document for index: #{index_alias()}. Reason: #{
              inspect(error)
            }"
          )

          {:error, false}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Runs a query against the index and returns the data that was found.
  """
  def query(query_data) do
    if elasticsearch_enabled?() do
      case Elasticsearch.query(index_alias(), query_data) do
        {:ok, search_results, _total} ->
          # TODO: This slows down the process by having to itterate through
          # the entire list of search results again. See if there is a way to
          # store or retrieve data from elastic and get the keys as atoms
          updated_search_results =
            Enum.reduce(search_results, [], fn result, acc ->
              atom_map = for {key, val} <- result, into: %{}, do: {String.to_atom(key), val}

              acc ++ [atom_map]
            end)

          {:ok, updated_search_results}

        {:error, error} ->
          Logger.error(
            "Elastic Query Error: Failed to query Elasticsearch index: #{index_alias()}. Reason: #{
              inspect(error)
            }"
          )

          {:error, error}
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  def query_event_definition_event_count(event_definition_ids) do
    if elasticsearch_enabled?() do
      aggregation_key = "event_definition"
      aggregation_sub_key = "event_count"

      query = %{
        size: 0,
        aggregations: %{
          event_definition: %{
            terms: %{
              field: "event_definition_id.keyword",
              include: event_definition_ids
            },
            aggs: %{
              event_count: %{
                cardinality: %{
                  field: "event_id.keyword"
                }
              }
            }
          }
        }
      }

      # TODO: Mock elasticsearch method
      with {:ok, response} <-
             Elasticsearch.query_aggs(index_alias(), query,
               aggregation_key: aggregation_key,
               aggregation_sub_key: aggregation_sub_key
             ) do
        Enum.reduce(response, %{}, fn
          %{
            "event_count" => event_count,
            "event_definition" => event_definition_id
          },
          acc ->
            Map.put(acc, event_definition_id, event_count)
        end)
      else
        {:error, error} ->
          Logger.error(
            "Event Count Failure: There was a failure when querying event count for event_definitions: #{
              inspect(error)
            }"
          )

          nil
      end
    else
      Logger.warn("Elasticsearch Disabled: Elasticsearch is not enabled for this environment")

      {:ok, :elasticsearch_not_enabled}
    end
  end

  @doc """
  Will build and return the map that represents the event_document. Takes in the Raw Event data, event detail field_name
  and field_value, the notification_setting title and event_definition_id, the created_at, updated_at and event_id for the event
  and the Crud action key
  """
  def build_document(
        event,
        field_name,
        field_value,
        %{title: title, id: event_definition_id},
        event_id,
        action
      ) do
    {document_id, published_by} =
      case event["published_by"] do
        nil ->
          # create a document_id that is the event_id + the hash of the field__name
          {"#{event_id}#{url_encoded_hash_256(field_name)}", nil}

        published_by ->
          # create a document_id that is the published_by + the hash of the field__name
          {"#{published_by}#{url_encoded_hash_256(field_name)}", published_by}
      end

    published_at =
      case event["published_at"] do
        nil -> DateTime.utc_now()
        published_at -> published_at
      end

    %{
      :id => document_id,
      :created_at => DateTime.truncate(DateTime.utc_now(), :second),
      :updated_at => DateTime.truncate(DateTime.utc_now(), :second),
      :event_id => event_id,
      :published_by => published_by,
      :title => title,
      :field_name => field_name,
      :field_value => field_value,
      :published_at => published_at,
      :event_definition_id => event_definition_id
    }
    |> Map.put_new_lazy(:action, fn ->
      if action == nil do
        crud_create_value()
      else
        action
      end
    end)
  end

  @doc """
  Creates a list of all the elasticsearch document ids that need to be deleted
  based on the published_by id that is passed in
  """
  def build_document_ids(published_by, %{fields: fields}) do
    Enum.reduce(fields, [], fn {field_name, _field_value}, acc ->
      # create a document_id that is the published_by + the hash of the field__name
      acc ++ ["#{published_by}#{url_encoded_hash_256(field_name)}"]
    end)
  end

  @doc """
  Will take a map %{field: ".....", value: "..."} and builds the
  term query to run against the elasticsearch index.
  """
  def build_term_query(%{field: field, value: value}) do
    %{
      query: %{
        term: %{
          "#{field}.keyword" => %{
            value: "#{value}",
            boost: "1.0"
          }
        }
      }
    }
  end

  @doc """
   Will take a map %{field: ".....", values: ["...", "...."]} and builds the
   terms query to run against the elasticsearch index.
  """
  def build_terms_query(%{field: field, values: values}) do
    %{
      query: %{
        terms: %{
          "#{field}.keyword" => values,
          boost: "1.0"
        }
      }
    }
  end

  @doc """
  Builds a wildcard query based on the args map
  """
  def build_wildcard_query(args) do
    query = %{}

    Enum.reduce(args, query, fn
      {:limit, limit}, query when is_integer(limit) ->
        Map.put_new(query, :size, limit)

      {:offset, offset}, query when is_integer(offset) ->
        Map.put_new(query, :from, offset)

      {:filter, %{matching: search_term}}, query ->
        search_query = %{
          :wildcard => %{
            :field_value => %{
              :value => "*#{search_term}*"
            }
          }
        }

        Map.put_new(query, :query, search_query)

      _, query ->
        query
    end)
  end

  # ----------------------#
  # --- Helper Methods ---#
  # ----------------------#
  defp today_date(), do: Timex.now() |> Timex.format!("%Y%m%d", :strftime)

  defp convert_keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end

  # Hashes the value and URL encodes it
  defp url_encoded_hash_256(value) do
    :crypto.hash(:sha256, value)
    |> Base.encode64()
    |> URI.encode_www_form()
  end

  # ------------ #
  # ---Configs-- #
  # ------------ #
  defp config(app, key), do: Application.get_env(app, key)

  defp index_alias(),
    do: config(:cogynt_workstation_ingest, __MODULE__)[:index_alias]

  defp elasticsearch_enabled?(), do: config(:elasticsearch, :config)[:enabled] || false

  defp crud_create_value(),
    do: Application.get_env(:cogynt_workstation_ingest, :core_keys)[:create]
end
