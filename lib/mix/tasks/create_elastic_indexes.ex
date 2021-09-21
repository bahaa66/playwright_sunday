defmodule Mix.Tasks.CreateElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session and run commands to manually create the
  necessary indexes for CogyntWorkstationIngest
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.API

  @impl Mix.Task
  def run(_) do
    with {:ok, _} <- HTTPoison.start(),
         {:ok, []} <- API.latest_index_starting_with("event_test"),
         :ok <-
           Elasticsearch.Index.create_from_file(
             CogyntWorkstationIngest.Elasticsearch.Cluster,
             "event_test",
             "priv/elasticsearch/event.json"
           ) do
      Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been created.")
    else
      {:error, _} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "An error occured trying to create the index #{Config.event_index_alias()}"
        )

        Mix.raise("""
          An error occured trying to create the index #{Config.event_index_alias()}
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to create the indexes.
        """)
    end
  end
end
