use Mix.Config

# Session Configurations
config :cogynt_workstation_ingest,
  session_domain: System.get_env("COGYNT_SESSION_DOMAIN") || "localhost",
  session_key: System.get_env("COGYNT_AUTH_SESSION_KEY") || "_cogynt_auth_key",
  signing_salt: System.get_env("COGYNT_AUTH_SALT") || "I45Kpw9a"

config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  load_from_system_env: true,
  url: [host: System.get_env("COGYNT_DOMAIN") || "localhost"],
  secret_key_base:
    System.get_env("COGYNT_SECRET_KEY_BASE") ||
      "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub_server: CogyntWorkstationIngestWeb.PubSub,
  https: [
    port: (System.get_env("HTTPS_PORT") || "450") |> String.to_integer(),
    otp_app: :cogynt_workstation_ingest,
    keyfile: System.get_env("TLS_KEY_PATH") || "",
    certfile: System.get_env("TLS_CERT_PATH") || ""
  ],
  http: [port: (System.get_env("HTTP_PORT") || "4002") |> String.to_integer()],
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  server: true,
  watchers: [],
  live_view: [signing_salt: System.get_env("COGYNT_AUTH_SALT") || "I45Kpw9a"]

# Kafka Configurations
config :cogynt_workstation_ingest, :kafka,
  # Dev Kafka
  # brokers: [
  #   {
  #     System.get_env("KAFKA_BROKER") || "kafka-dst.cogilitycloud.com",
  #     (System.get_env("KAFKA_PORT") || "9092") |> String.to_integer()
  #   }
  # ],
  brokers: [{"127.0.0.1", 9092}],
  audit_topic: System.get_env("AUDIT_LOG_TOPIC") || "_cogynt_audit_log",
  template_solution_topic: System.get_env("TEMPLATE_SOLUTION_TOPIC") || "template_solutions",
  template_solution_event_topic:
    System.get_env("TEMPLATE_SOLUTION_EVENT_TOPIC") || "template_solution_events",
    deployment_topic: System.get_env("DEPLOYMENT_TOPIC") || "deployment",
  topic_partitions: System.get_env("TOPIC_PARTITIONS") || 10,
  topic_replication: System.get_env("TOPIC_REPLICATION") || 1,
  topic_config: System.get_env("TOPIC_CONFIG") || []

# Elasticsearch configurations
config :elasticsearch, :config,
  enabled: System.get_env("ELASTICSEARCH_ENABLED") || true,
  basic_authentication_enabled: true,
  host: System.get_env("ELASTIC_URL") || "http://localhost:9200",
  username: System.get_env("ELASTIC_USERNAME") || "elasticsearch",
  password: System.get_env("ELASTIC_PASSWORD") || "elasticsearch",
  event_index_alias: System.get_env("EVENT_INDEX_ALIAS") || "event",
  risk_history_index_alias: System.get_env("RISK_HISTORY_INDEX_ALIAS") || "risk_history",
  retry_on_conflict: 5,
  utc_offset: 0

# Redis configurations
config :redis, :application,
  host: System.get_env("COGYNT_REDIS_HOST") || "127.0.0.1",
  port: 6379,
  password: System.get_env("COGYNT_REDIS_PASSWORD") || nil,
  name: System.get_env("COGYNT_REDIS_NAME") || "",
  sentinel: System.get_env("COGYNT_REDIS_SENTINEL") || "",
  databse: System.get_env("COGYNT_REDIS_DATABASE") || "",
  pools: System.get_env("COGYNT_REDIS_POOLS") || 5,
  exit_on_disconnection: System.get_env("COGYNT_REDIS_EXIT_ON_DISCONNECTION") || true,
  sync_connect: System.get_env("COGYNT_REDIS_SYNC_CONNECT") || true,
  instance: System.get_env("COGYNT_REDIS_INSTANCE") || :single

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  processor_stages:
    (System.get_env("EVENTPIPELINE_PROCESSOR_STAGES") || "10") |> String.to_integer()

config :cogynt_workstation_ingest, :drilldown_pipeline,
  processor_stages: (System.get_env("DRILLDOWN_PROCESSOR_STAGES") || "20") |> String.to_integer()

config :cogynt_workstation_ingest, :deployment_pipeline,
  processor_stages: (System.get_env("DEPLOYMENT_PROCESSOR_STAGES") || "2") |> String.to_integer()

# TODO: Depricated
config :cogynt_workstation_ingest, :drilldown_producer,
  max_retry: System.get_env("DRILLDOWN_MAX_RETRY") || 1_400,
  time_delay: System.get_env("DRILLDOWN_TIME_DELAY") || 60_000,
  allowed_messages: System.get_env("PRODUCER_ALLOWED_MESSAGES") || "30000" |> String.to_integer(),
  rate_limit_interval:
    System.get_env("PRODUCER_RATE_LIMIT_INTERVAL") || "30000" |> String.to_integer()

# TODO: Depricated
config :cogynt_workstation_ingest, :producer,
  max_retry: System.get_env("PRODUCER_MAX_RETRY") || 1_400,
  time_delay: System.get_env("PRODUCER_TIME_DELAY") || 60_000,
  allowed_messages: System.get_env("PRODUCER_ALLOWED_MESSAGES") || "30000" |> String.to_integer(),
  rate_limit_interval:
    System.get_env("PRODUCER_RATE_LIMIT_INTERVAL") || "30000" |> String.to_integer()

config :cogynt_workstation_ingest, :consumer_retry_cache,
  time_delay: System.get_env("CONSUMER_RETRY_CACHE_TIME_DELAY") || 600_000,
  max_retry: System.get_env("CONSUMER_RETRY_CACHE_MAX_RETRY") || 144

config :cogynt_workstation_ingest, :deployment_retry_cache,
  time_delay: System.get_env("DEPLOYMENT_RETRY_CACHE_TIME_DELAY") || 30_000,
  max_retry: System.get_env("DEPLOYMENT_RETRY_CACHE_MAX_RETRY") || 2880

# startup utils configurations
config :cogynt_workstation_ingest, :startup, init_delay: System.get_env("INIT_DELAY") || 1000

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime for faster development compilation
config :phoenix, :plug_init_mode, :runtime

# Configure your database
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
  username: System.get_env("POSTGRESQL_USERNAME") || "postgres",
  password: System.get_env("POSTGRESQL_PASSWORD") || "postgres",
  database: System.get_env("POSTGRESQL_DATABASE") || "cogynt_dev",
  hostname: System.get_env("POSTGRESQL_HOST") || "localhost",
  pool_size: (System.get_env("POSTGRESQL_POOL_SIZE") || "50") |> String.to_integer(),
  telemetry_prefix: [:cogynt_workstation_ingest, :repo]
