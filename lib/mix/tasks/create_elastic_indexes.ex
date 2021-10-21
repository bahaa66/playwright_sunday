defmodule Mix.Tasks.CreateElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session and run commands to manually create the
  necessary indexes for CogyntWorkstationIngest
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.ElasticsearchAPI

  @impl Mix.Task
  def run(_) do
       with {:ok, _} <- HTTPoison.start(),
       {:ok, _} <- Application.ensure_all_started(:ecto),
       {:ok, _} <- Application.ensure_all_started(:postgrex),
       {:ok, _} <- CogyntWorkstationIngest.Repo.start_link(),
       {:ok, _} <- CogyntWorkstationIngest.Elasticsearch.Cluster.start_link(),
       {:ok, false} <- ElasticsearchAPI.index_exists?(Config.event_index_alias()),
       {:ok, _ } <- ElasticsearchAPI.create_index(Config.event_index_alias()) do
        Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been created.")
      else
        {:ok, true} ->
          ElasticsearchAPI.check_to_reindex()
          #TBD: should you stop the services that were started?
          # [:connection, :db_connection, :decimal, :ecto, :postgrex]
          Mix.shell().info("The index: #{Config.event_index_alias()} already exists.")

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
