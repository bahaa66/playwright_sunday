defmodule Mix.Tasks.DropElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session to manually drop the
  necessary indexes when testing out a new client deployment.
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.API
  CogyntWorkstationIngest.Elasticsearch.Cluster

  @impl Mix.Task
  def run(_) do
    with {:ok, _} <- HTTPoison.start(),
         {:ok, index} <- API.latest_index_starting_with("event_test") do
      Elasticsearch.delete(Cluster, index)
      Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been deleted.")
    else
      {:error, _} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "An error occured trying to delete the index #{Config.event_index_alias()}"
        )

        Mix.raise("""
          An error occured trying to delete the index #{Config.event_index_alias()}
        """)

      _ ->
        Mix.raise("""
          An unexpected error occurred trying to delete the indexes.
        """)
    end
  end
end
