defmodule CogyntWorkstationIngest.ReleaseTasks do
  alias CogyntWorkstationIngest.ElasticsearchAPI
  alias CogyntWorkstationIngest.Config


  @apps [
    :cogynt_workstation_ingest
  ]

  @start_apps [
    :postgrex,
    :ecto_sql,
    :ecto,
    :elasticsearch,
    :httpoison
  ]

  def repos(app), do: Application.get_env(app, :ecto_repos, []) |> IO.inspect()

  def premigrate do
    start_services()

    run_migrations()

    stop_services()
  end

  def elasticindexes do
    start_services()

    run_indexes()

    stop_services()
  end

  def migrate do
    {:ok, _} = Application.ensure_all_started(:cogynt_workstation_ingest)
    path = Application.app_dir(:cogynt, "priv/repo/migrations")
    Ecto.Migrator.run(CogyntWorkstationIngest.Repo, path, :up, all: true)
  end

  def reset do
    {:ok, _} = Application.ensure_all_started(:cogynt_workstation_ingest)
    path = Application.app_dir(:cogynt, "priv/repo/migrations")
    Ecto.Migrator.run(CogyntWorkstationIngest.Repo, path, :down, all: true)
  end

  def start_services do
    for app <- @apps do
      # Start apps necessary for executing migrations
      IO.puts("Starting dependencies..")
      Enum.each(@start_apps, &Application.ensure_all_started/1)

      # Start the Repo(s) for app
      IO.puts("Starting repos..")
      Enum.each(repos(app), & &1.start_link(pool_size: 2))
    end
  end

  def stop_services do
    IO.puts("Success!")
    :init.stop()
  end

  defp run_migrations() do
    IO.puts("Running migrations..")

    for app <- @apps do
      Enum.each(repos(app), &run_migrations_for/1)
    end
  end

  defp run_indexes() do
    IO.puts("Running indexes..")

    with {:ok, _} <- HTTPoison.start(),
         {:ok, false} <- ElasticsearchAPI.index_exists?(Config.event_index_alias()) do
          ElasticsearchAPI.create_index(Config.event_index_alias())
         IO.puts("The event_index for CogyntWorkstation has been created.")
         IO.puts("indexes complete..")
    else
      {:ok, true} ->
        ElasticsearchAPI.check_to_reindex()
      {:error, %Elasticsearch.Exception{raw: %{"error" => error}}} ->
        reason = Map.get(error, "reason")
        IO.puts("Failed to create event index #{reason}")
    end
  end

  defp run_migrations_for(repo) do
    app = Keyword.get(repo.config, :otp_app)
    IO.puts("Running migrations for #{app}..")
    migrations_path = priv_path_for(repo, "migrations")
    Ecto.Migrator.run(repo, migrations_path, :up, all: true)
  end

  def priv_path_for(repo, filename) do
    app = Keyword.get(repo.config, :otp_app)
    repo_underscore = repo |> Module.split() |> List.last() |> Macro.underscore()
    Path.join([priv_dir(app), repo_underscore, filename])
  end

  def priv_dir(app), do: "#{:code.priv_dir(app)}"
end
