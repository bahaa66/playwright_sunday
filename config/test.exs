use Mix.Config

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  http: [port: 4002],
  server: false

config :kafka, :application,
  brokers: [{"127.0.0.1", 9092}],
  # kafka_client: TODO
  audit_topic: "_cogynt_audit_log_test",
  template_solutions_topic: "template_solutions_test",
  template_solution_events_topic: "template_solution_events_test",
  deployment_topic: "deployment_test",
  partition_strategy: :random,
  partitions: 1,
  replication_factor: 1,
  replica_assignment: [],
  config_entries: [],
  session_timeout: 10000

# Print only warnings and errors during test
config :logger, level: :warn

# Configure your database
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
  username: "postgres",
  password: "postgres",
  database: "cogynt_test",
  hostname: "localhost",
  pool: Ecto.Adapters.SQL.Sandbox
