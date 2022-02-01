defmodule CogyntWorkstationIngest.ReleaseTasks do
  @app :cogynt_workstation_ingest
  alias CogyntWorkstationIngest.ElasticsearchAPI
  alias CogyntWorkstationIngest.Config

  @deps [
    :elasticsearch,
    :httpoison
  ]

  def eval_elasticsearch do
    start_services()
    create_elastic_deps()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp create_elastic_deps() do
    IO.puts("Creating Elastic Indexes..")

    with {:ok, _} <- HTTPoison.start(),
         {:ok, false} <- ElasticsearchAPI.index_exists?(Config.event_index_alias()) do
      ElasticsearchAPI.create_index(Config.event_index_alias())
      IO.puts("The Index: #{Config.event_index_alias()} for CogyntWorkstation has been created.")
      IO.puts("Indexes complete..")
    else
      {:ok, true} ->
        ElasticsearchAPI.check_to_reindex()
        IO.puts("Reindexing Check complete..")

      {:error, %Elasticsearch.Exception{raw: %{"error" => error}}} ->
        reason = Map.get(error, "reason")
        IO.puts("Failed to Create #{Config.event_index_alias()} Index: #{reason}")

      {:error, error} ->
        IO.puts(
          "Failed to Create #{Config.event_index_alias()} Index: #{inspect(error, pretty: true)}"
        )
    end
  end

  defp start_services do
    # Start deps
    IO.puts("Starting dependencies..")
    Enum.each(@deps, &Application.ensure_all_started/1)
  end
end
