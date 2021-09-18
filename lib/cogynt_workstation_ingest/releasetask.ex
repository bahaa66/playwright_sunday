defmodule CogyntWorkstationIngest.ReleaseTasks do
  alias CogyntWorkstationIngest.Elasticsearch.API

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

  # alias Elasticsearch.IndexMappings.EventIndexMapping
  # alias CogyntWorkstationIngest.Config

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
         {:ok, _index}  <- API.index_starting_with("event_test"),
         :ok <-
           Elasticsearch.Index.create_from_file(
             CogyntWorkstationIngest.Elasticsearch.Cluster,
             "event_test",
             "priv/elasticsearch/event.json"
           ) do
      Elasticsearch.Index.alias(
        CogyntWorkstationIngest.Elasticsearch.Cluster,
        "event_test",
        "event_test"
      )

      IO.puts("The event_index for CogyntWorkstation have been created.")

      IO.puts("indexes complete..")
    else
      {:error, %Elasticsearch.Exception{raw: %{"error" => error}}} ->
        reason = Map.get(error, "reason")
        IO.puts("Failed to create event index #{reason}")

      # config = Elasticsearch.Cluster.Config.get(CogyntWorkstationIngest.Elasticsearch.Cluster) |> IO.inspect()
      # alias = String.to_existing_atom("event_test") |> IO.inspect()
      # name = Elasticsearch.Index.build_name(alias)|> IO.inspect()
      # %{settings: settings_file} = index_config = config[:indexes][alias]

      # with :ok <- Elasticsearch.Index.create_from_file(config, name, settings_file),
      #      bulk_upload(config, name, index_config),
      #      :ok <- Elasticsearch.Index.alias(config, name, alias),
      #      :ok <- Elasticsearch.Index.clean_starting_with(config, alias, 2),
      #      :ok <- Elasticsearch.Index.refresh(config, name) do
      #       :ok
      # else
      #     err -> IO.puts(err)

      {:ok, true} ->
        IO.puts("event_index already exists.")
        IO.puts("indexes complete..")
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
