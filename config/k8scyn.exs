import Config

config :cogynt_workstation_ingest,
  # event_pipeline_batch_size: 10,
  drilldown_enabled: false

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo, pool_size: 20

config :libcluster,
  debug: true

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime for faster development compilation
config :phoenix, :plug_init_mode, :runtime

config :cogynt_graphql, :common,
  mock_license: true,
  mock_license_status: "licensed"
