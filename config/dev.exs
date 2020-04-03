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
  pubsub: [name: CogyntWorkstationIngestWeb.PubSub, adapter: Phoenix.PubSub.PG2],
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
  http_client: System.get_env("HTTP_CLIENT") || HTTPoison

# Environment configurations
config :cogynt_workstation_ingest, env: (System.get_env("ENV") || "dev") |> String.to_atom()

# Kafka Configurations
config :kafka_ex,
  # Dev Kafka
  brokers: [
    {
      System.get_env("KAFKA_BROKER") || "172.16.1.100",
      (System.get_env("KAFKA_PORT") || "9092") |> String.to_integer()
    }
  ],
  # Local Kafka
  # brokers: [{"127.0.0.1", 9092}],
  auto_offset_reset: :earliest,
  kafka_version: "2.0",
  commit_interval: System.get_env("KAFKA_COMMIT_INTERVAL") || 1000,
  commit_threshold: System.get_env("KAFKA_COMMIT_THRESHOLD") || 1000,
  heartbeat_interval: System.get_env("KAFKA_HEARTBEAT_INTERVAL") || 1000,
  kafka_client: System.get_env("KAFKA_CLIENT") || KafkaEx,
  audit_topic: System.get_env("AUDIT_LOG_TOPIC") || "cogynt_audit_log",
  template_solution_topic: System.get_env("TEMPLATE_SOLUTION_TOPIC") || "template_solutions",
  template_solution_event_topic:
    System.get_env("TEMPLATE_SOLUTION_EVENT_TOPIC") || "template_solution_events",
  topic_partitions: System.get_env("TOPIC_PARTITIONS") || 1,
  topic_replication: System.get_env("TOPIC_REPLICATION") || 1,
  topic_config: System.get_env("TOPIC_CONFIG") || []

# Elasticsearch configurations
config :elasticsearch, :config,
  enabled: System.get_env("ELASTICSEARCH_ENABLED") || true,
  basic_authentication_enabled: true,
  host: System.get_env("ELASTIC_URL") || "http://localhost:9200",
  username: System.get_env("ELASTIC_USERNAME") || "elasticsearch",
  password: System.get_env("ELASTIC_PASSWORD") || "elasticsearch",
  elasticsearch_client: System.get_env("ELASTIC_CLIENT") || Elasticsearch,
  event_index_alias: System.get_env("EVENT_INDEX_ALIAS") || "event",
  risk_history_index_alias: System.get_env("RISK_HISTORY_INDEX_ALIAS") || "risk_history",
  utc_offset: 0

# Configurations for keys in Cogynt Core events
config :cogynt_workstation_ingest, :core_keys,
  crud: System.get_env("CORE_KEYS_CRUD") || "$crud",
  risk_score: System.get_env("CORE_KEYS_RISK_SCORE") || "_confidence",
  partial: System.get_env("CORE_KEYS_PARTIAL") || "$partial",
  events: System.get_env("CORE_KEYS_EVENTS") || "$$events",
  description: System.get_env("CORE_KEYS_DESCRIPTION") || "$description",
  entities: System.get_env("CORE_KEYS_ENTITIES") || "$$entities",
  link_data_type: System.get_env("CORE_KEYS_LINK_DATA_TYPE") || "linkage",
  update: System.get_env("CORE_KEYS_UPDATE") || "update",
  delete: System.get_env("CORE_KEYS_DELETE") || "delete",
  create: System.get_env("CORE_KEYS_CREATE") || "create"

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.EventPipeline,
  processor_stages: System.get_env("EVENTPIPELINE_PROCESSOR_STAGES") || 10,
  processor_max_demand: System.get_env("EVENTPIPELINE_PROCESSOR_MAX_DEMAND") || 100,
  processor_min_demand: System.get_env("EVENTPIPELINE_PROCESSOR_MIN_DEMAND") || 10

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.LinkEventPipeline,
  processor_stages: System.get_env("LINKEVENTPIPELINE_PROCESSOR_STAGES") || 10,
  processor_max_demand: System.get_env("LINKEVENTPIPELINE_PROCESSOR_MAX_DEMAND") || 100,
  processor_min_demand: System.get_env("LINKEVENTPIPELINE_PROCESSOR_MIN_DEMAND") || 10

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.DrilldownPipeline,
  processor_stages: System.get_env("DRILLDOWNPIPELINE_PROCESSOR_STAGES") || 3,
  processor_max_demand: System.get_env("DRILLDOWNPIPELINE_PROCESSOR_MAX_DEMAND") || 100,
  processor_min_demand: System.get_env("DRILLDOWNPIPELINE_PROCESSOR_MIN_DEMAND") || 10

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.Producer,
  max_retry: System.get_env("PRODUCER_MAX_RETRY") || 1_400,
  time_delay: System.get_env("PRODUCER_TIME_DELAY") || 60_000

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.DrilldownProducer,
  max_retry: System.get_env("DRILLDOWNPIPELINE_PRODUCER_MAX_RETRY") || 1400,
  time_delay: System.get_env("DRILLDOWNPIPELINE_PRODUCER_TIME_DELAY") || 60000

# ConsumerRetryCache Configurations
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache,
  time_delay: System.get_env("CONSUMER_RETRY_CACHE_TIME_DELAY") || 600_000,
  max_retry: System.get_env("CONSUMER_RETRY_CACHE_MAX_RETRY") || 144

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Servers.Caches.DrilldownCache,
  time_delay: System.get_env("DRILLDOWN_CACHE_TIME_DELAY") || 1_000

# rpc server/client configurations
config :cogynt_workstation_ingest, :rpc,
  cogynt_otp_service_name: System.get_env("COGYNT_OTP_SERVICE_NAME") || "http://localhost",
  cogynt_otp_service_port: System.get_env("COGYNT_OTP_SERVICE_PORT") || 4010

# startup utils configurations
config :cogynt_workstation_ingest, :startup, init_delay: System.get_env("INIT_DELAY") || 5000

# Do not include metadata nor timestamps in development logs
config :logger,
  level: (System.get_env("LOG_LEVEL") || "debug") |> String.to_atom()

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
  pool_size: 10
