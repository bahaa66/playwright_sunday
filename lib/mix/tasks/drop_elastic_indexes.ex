defmodule Mix.Tasks.DropElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session to manually drop the
  necessary indexes when testing out a new client deployment.
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.Cluster
  alias Elasticsearch.Index

  @impl Mix.Task
  def run(_) do
    with {:ok, _} <- HTTPoison.start(),
         {:ok, index} <- Index.latest_starting_with(Cluster, Config.event_index_alias()) do
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
