import Config

config :cogynt_workstation_ingest,
  # event_pipeline_batch_size: 10,
  drilldown_enabled: true

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo, pool_size: 20

# Currently not using Libcluster because of Istio blocker
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

config :pinot, :common,
  controller_url: "https://pinot-dev1.cogilitycloud.com",
  broker_url: "https://pinot-broker-dev1.cogilitycloud.com:443",
  kafka_broker_list: "kafka.cogynt.svc.cluster.local:9071",
  schema_registry_url: "http://schemaregistry.cogynt.svc.cluster.local:8081",
  controller_service: Pinot.Controller,
  broker_service: Pinot.Broker
