defmodule CogyntWorkstationIngest.MixProject do
  use Mix.Project

  def project do
    [
      app: :cogynt_workstation_ingest,
      version: "0.1.0",
      elixir: "~> 1.8",
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
      extra_applications: [:logger, :runtime_tools, :scrivener_ecto]
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
      {:phoenix, "~> 1.5.0", override: true},
      {:phoenix_pubsub, "~> 2.0"},
      {:phoenix_live_dashboard, "~> 0.1"},
      {:distillery, "~> 2.1"},
      {:phoenix_ecto, "~> 4.0"},
      {:ecto_sql, "~> 3.0"},
      {:postgrex, ">= 0.0.0"},
      {:gettext, "~> 0.11"},
      {:jason, "~> 1.0"},
      {:ja_serializer, "~> 0.13.0"},
      {:uuid, "~> 1.1"},
      {:plug, "~> 1.8"},
      {:cowboy, "~> 2.6"},
      {:plug_cowboy, "~> 2.0"},
      {:health_checkup, "~> 0.1.0"},
      {:ecto_enum, "~> 1.4"},
      {:broadway_kafka, "~> 0.1.0", override: true},
      {:httpoison, "~> 1.7"},
      {:junit_formatter, "~> 3.0"},
      {:dialyxir, "~> 0.5", only: [:dev], runtime: false},
      {:scrivener_ecto, "~> 2.0"},
      {:quiet_logger, "~> 0.2.0"},
      {:telemetry, "~> 0.4.0"},
      {:telemetry_poller, "~> 0.4"},
      {:kafka,
       git: "git@github.com:cogility/cogynt-common.git",
       sparse: "kafka",
       tag: "v1.8.5-beta",
       override: true},
      {
        :models,
        tag: "v1.8.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "models",
        override: true
      },
      {
        :migrations,
        tag: "v1.8.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "migrations",
        override: true
      },
      {
        :utils,
        tag: "v1.8.0",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "utils",
        override: true
      },
      {
        :elasticsearch,
        #tag: "v1.8.9-beta",
        branch: "feature/CDST-720-redefine-event-index",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "elasticsearch",
        override: true
      },
      {
        :redis,
        tag: "v1.8.2-beta",
        git: "git@github.com:cogility/cogynt-common.git",
        sparse: "redis",
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
        "run priv/repo/seeds.exs",
        "create_elastic_indexes"
      ],
      "ecto.reset": ["drop_elastic_indexes", "flush_redis_db", "ecto.drop", "ecto.setup"],
      test: ["ecto.create --quiet", "ecto.migrate", "test"]
    ]
  end
end
