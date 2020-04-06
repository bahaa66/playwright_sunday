defmodule CogyntWorkstationIngest.Elasticsearch.RiskHistoryDocument do
  @moduledoc """
  Document module for the RiskHistory index in elasticsearch
  """
  require Logger

  @confidence Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]

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
      properties: %{
        risk_history: %{
          type: "nested",
          properties: %{
            confidence: %{
              type: "text",
              fields: %{
                keyword: %{
                  type: "keyword"
                }
              }
            },
            event_id: %{
              type: "text",
              fields: %{
                keyword: %{
                  type: "keyword"
                }
              }
            },
            timestamp: %{
              type: "date",
              fields: %{
                keyword: %{
                  type: "keyword"
                }
              }
            }
          }
        }
      }
    }
  }

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
  builds the document for RiskHistoryDocument
  """
  def build_document(event_id, event) do
    case event["id"] do
      nil ->
        nil

      core_event_id ->
        case Elasticsearch.get_document(index_alias(), core_event_id) do
          {:ok, %{"risk_history" => risk_history}} ->
            validate_event_data(event_id, event, risk_history)

          {:error, _error} ->
            validate_event_data(event_id, event)
        end
    end
  end

  # ----------------------#
  # --- Helper Methods ---#
  # ----------------------#
  defp today_date(), do: Timex.now() |> Timex.format!("%Y%m%d", :strftime)

  defp convert_keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end

  defp validate_event_data(event_id, event, risk_history \\ []) do
    try do
      confidence = event[@confidence]
      timestamp = event["_timestamp"]

      with false <- is_nil(confidence) or confidence == "",
           false <- is_nil(event["_timestamp"]) or timestamp == "" do
        %{
          id: event["id"],
          risk_history:
            risk_history ++
              [
                %{
                  confidence: convert_risk_score_to_float(confidence),
                  timestamp: timestamp,
                  event_id: event_id
                }
              ]
        }
      else
        true ->
          nil
      end
    rescue
      _ ->
        nil
    end
  end

  defp convert_risk_score_to_float(confidence) when is_binary(confidence) do
    case Float.parse(confidence) do
      {float, _} ->
        float

      :error ->
        # TODO: Log bad risk_score data passed
        raise "string value passed in cannot be parsed as a float"
    end
  end

  defp convert_risk_score_to_float(confidence) when is_float(confidence) do
    confidence
  end

  defp convert_risk_score_to_float(confidence) when is_integer(confidence) do
    confidence / 1
  end

  # ------------ #
  # ---Configs-- #
  # ------------ #
  defp config(app, key), do: Application.get_env(app, key)

  defp index_alias(),
    do: config(:elasticsearch, :config)[:risk_history_index_alias]

  defp elasticsearch_enabled?(), do: config(:elasticsearch, :config)[:enabled] || false
end
