import Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  http: [port: 4002],
  server: false

config :kafka, :common,
  brokers: "127.0.0.1:9092",
  template_solutions_topic: "template_solutions_test",
  template_solution_events_topic: "template_solution_events_test",
  deployment_topic: "deployment_test",
  partition_strategy: :random,
  partitions: 1,
  replication_factor: 1,
  replica_assignment: [],
  config_entries: [],
  session_timeout: 10000

# Redis Configurations
config :redis, :common, client: CogyntWorkstationIngest.Clients.Redis.MockClient

config :elasticsearch, :common,
  service: CogyntWorkstationIngest.ElasticsearchMockService

# Print only warnings and errors during test
config :logger, level: :warn

# Configure your database
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
  username: "postgres",
  password: "postgres",
  database: "cogynt_test",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox

# Default libcluster configs.
config :libcluster,
  debug: true,
  topologies: [
    ingest: [
      strategy: Cluster.Strategy.Gossip
    ]
  ]
