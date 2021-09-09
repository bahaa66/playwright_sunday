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
config :kafka, :application,
  brokers: System.get_env("KAFKA_BROKERS") || "127.0.0.1:9092",
  partition_strategy: (System.get_env("PARTITION_STRATEGY") || "random") |> String.to_atom(),
  partitions: (System.get_env("PARTITIONS") || "10") |> String.to_integer(),
  replication_factor:
    (System.get_env("KAFKA_TOPIC_REPLICATION_FACTOR") || "1") |> String.to_integer(),
  replica_assignment: System.get_env("REPLICA_ASSIGNMENT") || [],
  config_entries: System.get_env("CONFIG_ENTRIES") || [],
  session_timeout: (System.get_env("SESSION_TIMEOUT") || "10000") |> String.to_integer(),
  kafka_connect_host: System.get_env("KAFKA_CONNECT_URL") || "http://localhost:8083"

# Elasticsearch configurations
config :elasticsearch, :application,
  cacertfile: System.get_env("ELASTIC_CA_CERT_PATH") || "",
  host: System.get_env("ELASTIC_URL") || "http://localhost:9200",
  username: System.get_env("ELASTIC_USERNAME") || "elasticsearch",
  password: System.get_env("ELASTIC_PASSWORD") || "elasticsearch",
  shards: (System.get_env("ELASTIC_SHARDS") || "1") |> String.to_integer(),
  replicas: (System.get_env("ELASTIC_REPLICAS") || "0") |> String.to_integer()

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.Cluster,
  username: System.get_env("ELASTIC_USERNAME") || "elasticsearch",
  password: System.get_env("ELASTIC_PASSWORD") || "elasticsearch",
  json_library: Jason,
  url: System.get_env("ELASTIC_URL") || "http://localhost:9200",
  api: Elasticsearch.API.HTTP,
  indexes: %{
    event_test: %{
      settings: "priv/elasticsearch/event.json",
      store: CogyntWorkstationIngest.Elasticsearch.Store,
      sources: [Models.Events.Event, Models.Events.EventDefinition, Models.EventDetailTemplates],
      bulk_page_size: 10,
      bulk_wait_interval: 5000
    }
  },
  default_options: [
    timeout: 10_000,
    recv_timeout: 5_000,
    hackney: [pool: :elasticsearh_pool],
    ssl: [versions: [:"tlsv1.2"]]
  ]

# Redis configurations
config :redis, :application,
  host: System.get_env("COGYNT_REDIS_HOST") || "127.0.0.1",
  password: System.get_env("COGYNT_REDIS_PASSWORD") || nil,
  name: System.get_env("COGYNT_REDIS_NAME") || "",
  sentinels: System.get_env("COGYNT_REDIS_SENTINELS") || "",
  sentinel_group: System.get_env("COGYNT_REDIS_SENTINEL_GROUP") || "main",
  database: System.get_env("COGYNT_REDIS_DATABASE") || "",
  pools: System.get_env("COGYNT_REDIS_POOLS") || 5,
  exit_on_disconnection: System.get_env("COGYNT_REDIS_EXIT_ON_DISCONNECTION") || true,
  sync_connect: System.get_env("COGYNT_REDIS_SYNC_CONNECT") || true,
  instance: (System.get_env("COGYNT_REDIS_INSTANCE") || "single") |> String.to_atom()

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
  # THIS SECTION IS MEANT FOR PROD/DEV CLUSTERS ONLY
  redis_options: [
    sentinel: [
      sentinels: String.split(System.get_env("COGYNT_REDIS_SENTINELS") || "", ",", trim: true),
      group: System.get_env("COGYNT_REDIS_SENTINEL_GROUP") || "main"
    ],
    name: Exq.Redis.Client,
    password: System.get_env("COGYNT_REDIS_PASSWORD") || nil
  ]

  # UNCOMMENT THIS SECTION, COMMENT OUT THE ABOVE SECTION WHEN DOING LOCAL DEVELOPMENT
  # redis_options: [
  #   host: System.get_env("COGYNT_REDIS_HOST") || "127.0.0.1",
  #   port: 6379,
  #   name: Exq.Redis.Client,
  #   password: System.get_env("COGYNT_REDIS_PASSWORD") || nil
  # ]

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  processor_stages:
    (System.get_env("EVENTPIPELINE_PROCESSOR_STAGES") || "10") |> String.to_integer(),
  producer_stages:
    (System.get_env("EVENTPIPELINE_PRODUCER_STAGES") || "10") |> String.to_integer()

config :cogynt_workstation_ingest, :deployment_pipeline,
  processor_stages: (System.get_env("DEPLOYMENT_PROCESSOR_STAGES") || "2") |> String.to_integer(),
  producer_stages: (System.get_env("DEPLOYMENT_PRODUCER_STAGES") || "2") |> String.to_integer()

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
  pool_size: (System.get_env("POSTGRESQL_POOL_SIZE") || "20") |> String.to_integer(),
  telemetry_prefix: [:cogynt_workstation_ingest, :repo]

config :druid,
  request_timeout: (System.get_env("DRUID_REQUEST_TIMEOUT") || "120000") |> String.to_integer(),
  query_priority: (System.get_env("DRUID_QUERY_PRIORITY") || "0") |> String.to_integer(),
  broker_profiles: [
    default: [
      base_url: System.get_env("DRUID_BASE_URL") || "http://localhost:8888",
      cacertfile: System.get_env("DRUID_CERT_FILE_PATH") || "path/to/druid-certificate.crt",
      http_username: System.get_env("DRUID_HTTP_USERNAME") || "username",
      http_password: System.get_env("DRUID_HTTP_PASSWORD") || "password"
    ]
  ],
  schema_registry_url: System.get_env("SCHEMA_REGISTRY_URL") || "http://schemaregistry:8081"
