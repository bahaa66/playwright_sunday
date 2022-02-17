defmodule Mix.Tasks.CreateElasticIndexes do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session and run commands to manually create the
  necessary indexes for CogyntWorkstationIngest
  """
  use Mix.Task

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi

  @impl Mix.Task
  def run(_) do
    Mix.Task.run("app.config")

    with {:ok, _} <- HTTPoison.start(),
         {:ok, _} <- CogyntWorkstationIngest.Elasticsearch.Cluster.start_link(),
         {:ok, false} <- ElasticApi.index_exists?(Config.event_index_alias()),
         {:ok, _status} <- ElasticApi.create_index(Config.event_index_alias()) do
      Mix.shell().info("The index: #{Config.event_index_alias()} for Cogynt has been created.")
    else
      {:ok, true} ->
        with {:ok, _, _} <-
               Ecto.Migrator.with_repo(
                 CogyntWorkstationIngest.Repo,
                 &Ecto.Migrator.run(&1, :up, all: true)
               ),
             {:ok, _} <- CogyntWorkstationIngest.Repo.start_link() do
          ElasticApi.check_to_reindex()
        else
          {:error, _} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "An error occured trying to reindex  #{Config.event_index_alias()}"
            )
        end

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
