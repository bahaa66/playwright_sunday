defmodule Mix.Tasks.DropElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session to manually drop the
  necessary indexes when testing out a new client deployment.
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config

  @impl Mix.Task
  def run(_) do
    with {:ok, _} <- HTTPoison.start(),
         {:ok, true} <- Elasticsearch.index_exists?(Config.event_index_alias()),
         {:ok, _} <- Elasticsearch.delete_index(Config.event_index_alias()) do
      Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been deleted.")
    else
      {:ok, false} ->
        Mix.shell().info("The #{Config.event_index_alias()} index does not exist.")

      {:ok, :elasticsearch_not_enabled} ->
        Mix.shell().info("Elasticsearch not enabled.")

      {:error, error} ->
        CogyntLogger.error(
          "Delete Elastic Index Failed",
          "An error occured trying to delete the index #{Config.event_index_alias()}. Error: #{
            inspect(error)
          }"
        )

        Mix.raise("""
          An error occured trying to delete the index #{Config.event_index_alias()}. Error: #{
          inspect(error)
        }
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to delete the indexes.
        """)
    end

    with {:ok, _} <- HTTPoison.start(),
         {:ok, true} <- Elasticsearch.index_exists?(Config.risk_history_index_alias()),
         {:ok, _} <- Elasticsearch.delete_index(Config.risk_history_index_alias()) do
      Mix.shell().info(
        "The index: #{Config.risk_history_index_alias()} for Cogynt has been deleted."
      )
    else
      {:ok, false} ->
        Mix.shell().info("The #{Config.risk_history_index_alias()} index does not exist.")

      {:ok, :elasticsearch_not_enabled} ->
        Mix.shell().info("Elasticsearch not enabled.")

      {:error, error} ->
        CogyntLogger.error(
          "Delete Elastic Index Failed",
          "An error occured trying to delete the index #{Config.risk_history_index_alias()}. Error: #{
            inspect(error)
          }"
        )

        Mix.raise("""
          An error occured trying to delete the index #{Config.risk_history_index_alias()}. Error: #{
          inspect(error)
        }
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to delete the indexes.
        """)
    end
  end
end
