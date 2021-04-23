use Mix.Config

config :cogynt_workstation_ingest,
  # Session Configurations
  session_domain: "localhost",
  session_key: "_cogynt_auth_key",
  signing_salt: "I45Kpw9a",
  # Dev Tool Configurations
  enable_dev_tools: true

config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  load_from_system_env: true,
  http: [port: 4002],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  server: true,
  watchers: [],
  live_view: [signing_salt: "I45Kpw9a"]

# Kafka Configurations
config :kafka, :application,
  brokers: "127.0.0.1:9092",
  partition_strategy: :random,
  partitions: 10,
  replication_factor: 1,
  replica_assignment: [],
  config_entries: [],
  session_timeout: 10000,
  kafka_connect_host: "http://localhost:8083"

# Elasticsearch configurations
config :elasticsearch, :application,
  cacertfile: "",
  host: "http://localhost:9200",
  username: "elasticsearch",
  password: "elasticsearch",
  shards: 1,
  replicas: 0

# Redis configurations
config :redis, :application,
  host: "127.0.0.1",
  password: nil,
  name: "",
  sentinels: "",
  sentinel_group: "main",
  databse: "",
  pools: 5,
  exit_on_disconnection: true,
  sync_connect: true,
  instance: :single

# Exq Job Queue
config :exq,
  name: Exq,
  node_identifier: CogyntWorkstationIngest.Utils.JobQueue.CustomNodeIdentifier,
  start_on_application: false,
  namespace: "exq",
  middleware: [
    Exq.Middleware.Stats,
    CogyntWorkstationIngest.Utils.JobQueue.Middleware.Job,
    Exq.Middleware.Manager,
    Exq.Middleware.Logger
  ],
  poll_timeout: 50,
  scheduler_poll_timeout: 200,
  scheduler_enable: true,
  max_retries: 1,
  mode: :default,
  shutdown_timeout: 15000,
  heartbeat_enable: true,
  heartbeat_interval: 60_000,
  missed_heartbeats_allowed: 5,
  redis_options: [
    host: "127.0.0.1",
    port: 6379,
    name: Exq.Redis.Client,
    password: nil
  ]

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  processor_stages: 10,
  producer_stages: 10

config :cogynt_workstation_ingest, :deployment_pipeline,
  processor_stages: 2,
  producer_stages: 2

config :cogynt_workstation_ingest, :drilldown_connector,
  ts_connector_name: "ts_drilldown_connector",
  tse_connector_name: "tse_drilldown_connector",
  time_delay: 600_000

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime for faster development compilation
config :phoenix, :plug_init_mode, :runtime

# Configure your database
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
  username: "postgres",
  password: "postgres",
  database: "cogynt_dev",
  hostname: "localhost",
  pool_size: 20,
  telemetry_prefix: [:cogynt_workstation_ingest, :repo]

config :druid,
  request_timeout: 120_000,
  query_priority: 0,
  broker_profiles: [
    default: [
      base_url: "http://localhost:8888",
      cacertfile: "path/to/druid-certificate.crt",
      http_username: "username",
      http_password: "password"
    ]
  ]

config :logger, :console, level: :info
