import Config

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo, pool_size: 20

# Default libcluster configs.
# Currently not using Libcluster because of Istio blocker
config :libcluster,
  debug: true,
  topologies: [
    ingest: [
      strategy: Cluster.Strategy.Gossip
    ]
  ]

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime for faster development compilation
config :phoenix, :plug_init_mode, :runtime

config :cogynt_graphql, :common, mock_license: true
