import Config

config :cogynt_workstation_ingest,
  drilldown_enabled: false

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

# The cyn env doesn't currently have pinot set up so we have to point to dev1
# If you enable drilldown know that you will be effecting the dev1 env.
config :pinot, :common,
  controller_url: "https://pinot-dev1.cogilitycloud.com",
  broker_url: "https://pinot-broker-dev1.cogilitycloud.com:443",
  kafka_broker_list: "kafka.cogynt.svc.cluster.local:9071",
  schema_registry_url: "http://schemaregistry.cogynt.svc.cluster.local:8081",
  controller_service: Pinot.Controller,
  broker_service: Pinot.Broker

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  # 5 mib
  max_bytes: 5_242_880,
  batch_size: 1000,
  # 10 sec
  batch_timeout: 10000
