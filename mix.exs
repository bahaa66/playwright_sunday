defmodule CogyntWorkstationIngest.MixProject do
  use Mix.Project

  def project do
    [
      app: :cogynt_workstation_ingest,
      version: "0.1.0",
      elixir: "~> 1.12",
      elixirc_paths: elixirc_paths(Mix.env()),
      compilers: [:phoenix, :gettext] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  # Configuration for the OTP application.
  #
  # Type `mix help compile.app` for more information.
  def application do
    [
      mod: {CogyntWorkstationIngest.Application, []},
      extra_applications: [:logger, :runtime_tools, :scrivener_ecto, :crypto, :ssl]
    ]
  end

  # Specifies which paths to compile per environment.
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Specifies your project dependencies.
  #
  # Type `mix help deps` for examples and options.
  defp deps do
    [
      {:phoenix, "~> 1.6.6"},
      {:phoenix_pubsub, "~> 2.0"},
      {:phoenix_live_dashboard, "~> 0.6.5"},
      {:distillery, "~> 2.1"},
      {:phoenix_ecto, "~> 4.0"},
      {:ecto_sql, "~> 3.7.1"},
      {:postgrex, "~> 0.15.8"},
      {:gettext, "~> 0.11"},
      {:jason, "~> 1.0"},
      {:ja_serializer, "~> 0.16.0"},
      {:jsonrpc2, "~> 2.0"},
      {:plug, "~> 1.8"},
      {:cowboy, "~> 2.6"},
      {:plug_cowboy, "~> 2.0"},
      {:health_checkup, "~> 0.1.0"},
      {:ecto_enum, "~> 1.4"},
      # {:broadway_kafka, "~> 0.3.0", override: true},
      {:broadway_kafka,
       branch: "main", git: "git@github.com:dashbitco/broadway_kafka.git", override: true},
      {:httpoison, "~> 1.7"},
      {:junit_formatter, "~> 3.0"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:scrivener_ecto, "~> 2.0"},
      {:quiet_logger, "~> 0.2.0"},
      {:telemetry_metrics, "~> 0.6"},
      {:telemetry_poller, "~> 1.0"},
      {:elasticsearch, "~> 1.0.0"},
      {:exq, git: "git@github.com:akira/exq.git", branch: "master"},
      {:libcluster, "~> 3.3.0"},
      {:horde, "~> 0.8.3"},
      {:absinthe, "~> 1.7.0"},
      {:absinthe_plug, "~> 1.5.5"},
      {:dataloader, "~> 1.0.0"},
      {:elixir_uuid, "~> 1.2"},
      {:ecto_psql_extras, "~> 0.6"},
      {:broadway_dashboard, "~> 0.2.0", override: true},
      {
        :kafka,
        tag: "v1.29.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "kafka",
        override: true
      },
      {
        :models,
        tag: "v1.29.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "models",
        override: true
      },
      {
        :migrations,
        #tag: "v1.29.0",
        branch: "feat/ingest-perf-testing",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "migrations",
        override: true
      },
      {
        :utils,
        tag: "v1.17.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "utils",
        override: true
      },
      {
        :redis,
        tag: "v1.20.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "redis",
        override: true
      },
      {
        :druid,
        tag: "v1.22.0", git: "git@github.com:cogility/cogynt-common.git", sparse: "druid"
      },
      {
        :cogynt_graphql,
        tag: "v1.27.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "cogynt_graphql",
        override: true
      },
      {
        :cogynt_elasticsearch,
        tag: "v1.26.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "cogynt-elasticsearch",
        override: true
      }
    ]
  end

  # Aliases are shortcuts or tasks specific to the current project.
  # For example, to create, migrate and run the seeds file at once:
  #
  #     $ mix ecto.setup
  #
  # See the documentation for `Mix` for more info on aliases.
  defp aliases do
    [
      "ecto.setup": [
        "ecto.create",
        "ecto.migrate",
        "run priv/repo/seeds.exs"
      ],
      "ecto.reset": ["flush_redis_db", "ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate", "test"]
    ]
  end
end
