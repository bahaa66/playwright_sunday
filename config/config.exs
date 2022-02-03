import Config

config :cogynt_workstation_ingest, :environment, config_env()

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo],
  session_domain: "localhost",
  session_key: "_cogynt_auth_key",
  signing_salt: "I45Kpw9a",
  enable_dev_tools: true,
  authoring_version: "1"

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub_server: CogyntWorkstationIngestWeb.PubSub,
  load_from_system_env: true,
  debug_errors: true,
  code_reloader: true,
  check_origin: false,
  server: true,
  watchers: [],
  live_view: [signing_salt: "I45Kpw9a"]

# Configure your database
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
  username: "postgres",
  password: "postgres",
  database: "cogynt_dev",
  hostname: "localhost",
  pool_size: 20,
  telemetry_prefix: [:cogynt_workstation_ingest, :repo]

# cogynt-common configurations
config :migrations, :application, repo: CogyntWorkstationIngest.Repo

config :cogynt_workstation_ingest, :clients,
  json_rpc_client: CogyntWorkstationIngestWeb.Clients.JsonRpcHTTPClient,
  http_client: HTTPoison,
  elasticsearch_client: Elasticsearch

# Kafka Configurations
config :kafka, :application,
  kafka_client: :brod,
  deployment_topic: "deployment",
  template_solutions_topic: "template_solutions",
  template_solution_events_topic: "template_solution_events"

# Elasticsearch Configurations
config :elasticsearch, :application,
  elasticsearch_client: Elasticsearch,
  http_client: HTTPoison,
  event_index_alias: "event",
  retry_on_conflict: 5,
  utc_offset: 0,
  host: "http://localhost:9200",
  username: "elasticsearch",
  password: "elasticsearch",
  shards: 1,
  replicas: 0

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.Cluster,
  username: "elasticsearch",
  password: "elasticsearch",
  json_library: Jason,
  url: "http://localhost:9200",
  api: Elasticsearch.API.HTTP,
  indexes: %{
    event: %{
      settings: "priv/elasticsearch/event.active.json",
      store: CogyntWorkstationIngest.Elasticsearch.Store,
      sources: [Models.Events.Event],
      bulk_page_size: 500,
      bulk_wait_interval: 0
    }
  },
  default_options: [
    timeout: 60_000,
    recv_timeout: 120_000,
    hackney: [pool: :elasticsearh_pool],
    ssl: [versions: [:"tlsv1.2"]]
  ]

# Redis Configurations
config :redis, :application,
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
  missed_heartbeats_allowed: 5

config :druid,
  request_timeout: 120000,
  query_priority: 0,
  broker_profiles: [
    default: [
      base_url: "http://localhost:8888",
      cacertfile: "path/to/druid-certificate.crt",
      http_username: "username",
      http_password: "password"
    ]
  ],
  schema_registry_url: "http://schemaregistry:8081"

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  processor_stages: 10,
  producer_stages: 10

config :cogynt_workstation_ingest, :deployment_pipeline,
  processor_stages: 2,
  producer_stages: 2

# Configurations for keys in Cogynt Core events
config :cogynt_workstation_ingest, :cogynt_keys,
  link_data_type: "linkage",
  update: "update",
  delete: "delete",
  create: "create",
  published_by: "published_by",
  published_at: "published_at",
  timestamp: "_timestamp",
  id: "id",
  version: "$version",
  crud: "$crud",
  confidence: "_confidence",
  partial: "$partial",
  entities: "$$entities",
  matches: "$matches",
  source: "source"

config :cogynt_workstation_ingest, :cogynt_keys_v2,
  link_data_type: "linkage",
  update: "update",
  delete: "delete",
  create: "create",
  published_by: "COG_published_by",
  published_at: "COG_published_at",
  timestamp: "COG_timestamp",
  id: "COG_id",
  version: "COG_version",
  crud: "COG_crud",
  confidence: "COG_confidence",
  partial: "COG_partial",
  entities: "COG_entities",
  matches: "COG_matches",
  source: "COG_source"

config :cogynt_workstation_ingest, :failed_messages,
  retry_timer: 300_000,
  max_retry: 5

config :cogynt_workstation_ingest, :consumer_retry_worker, retry_timer: 30_000

config :cogynt_workstation_ingest, :ingest_task_worker, timer: 1000

# RPC configurations
config :cogynt_workstation_ingest, :rpc,
  cogynt_auth_service_name: "http://localhost",
  cogynt_auth_service_port: 4999

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id],
  level: :info

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :phoenix, :format_encoders, "json-api": Jason

config :plug, :types, %{
  "application/vnd.api+json" => ["json-api"]
}

# Default libcluster configs.
config :libcluster,
  topologies: [
    ingest: [
      strategy: Cluster.Strategy.Gossip
    ]
  ]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
