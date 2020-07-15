defmodule Mix.Tasks.CreateElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session and run commands to manually create the
  necessary indexes for CogyntWorkstationIngest
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias Elasticsearch.IndexMappings.{EventIndexMapping, RiskHistoryIndexMapping}

  @impl Mix.Task
  def run(_) do
    with {:ok, _} <- HTTPoison.start(),
         {:ok, false} <- Elasticsearch.index_exists?(Config.event_index_alias()),
         {:ok, _} <-
           Elasticsearch.create_index(
             Config.event_index_alias(),
             EventIndexMapping.event_index_settings()
           ) do
      Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been created.")
    else
      {:ok, true} ->
        Mix.shell().info("The #{Config.event_index_alias()} index already exists.")

      {:ok, :elasticsearch_not_enabled} ->
        Mix.shell().info("Elasticsearch not enabled.")

      {:error, error} ->
        CogyntLogger.error(
          "Create Elastic Index Failed",
          "An error occured trying to create the index #{Config.event_index_alias()}. Error: #{
            inspect(error)
          }"
        )

        Mix.raise("""
          An error occured trying to create the index #{Config.event_index_alias()}. Error: #{
          inspect(error)
        }
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to create the indexes.
        """)
    end

    with {:ok, _} <- HTTPoison.start(),
         {:ok, false} <- Elasticsearch.index_exists?(Config.risk_history_index_alias()),
         {:ok, _} <-
           Elasticsearch.create_index(
             Config.risk_history_index_alias(),
             RiskHistoryIndexMapping.risk_history_index_settings()
           ) do
      Mix.shell().info(
        "The index: #{Config.risk_history_index_alias()} for Cogynt has been created."
      )
    else
      {:ok, true} ->
        Mix.shell().info("The #{Config.risk_history_index_alias()} index already exists.")

      {:ok, :elasticsearch_not_enabled} ->
        Mix.shell().info("Elasticsearch not enabled.")

      {:error, error} ->
        CogyntLogger.error(
          "Create Elastic Index Failed",
          "An error occured trying to create the index #{Config.risk_history_index_alias()}. Error: #{
            inspect(error)
          }"
        )

        Mix.raise("""
          An error occured trying to create the index #{Config.risk_history_index_alias()}. Error: #{
          inspect(error)
        }
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to create the indexes.
        """)
    end
  end
end
