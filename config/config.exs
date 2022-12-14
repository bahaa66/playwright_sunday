import Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo],
  session_domain: "localhost",
  session_key: "_cogynt_auth_key",
  signing_salt: "I45Kpw9a",
  enable_dev_tools: true,
  drilldown_enabled: true

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
  queue_target: 1_000,
  queue_interval: 5_000,
  telemetry_prefix: [:cogynt_workstation_ingest, :repo]

# cogynt-common configurations
config :models, :common, repo: CogyntWorkstationIngest.Repo

config :cogynt_workstation_ingest, :clients,
  json_rpc_client: CogyntWorkstationIngestWeb.Clients.JsonRpcHTTPClient,
  http_client: HTTPoison,
  elasticsearch_client: Elasticsearch

# Kafka Configurations
config :kafka, :common,
  kafka_client: :brod,
  deployment_topic: "deployment",
  template_solutions_topic: "template_solutions",
  template_solution_events_topic: "template_solution_events",
  audit_topic: "_cogynt_audit_log"

config :elasticsearch, :common,
  otp_app: :cogynt_workstation_ingest,
  username: "elasticsearch",
  password: "elasticsearch",
  url: "http://localhost:9200",
  indices: [
    event: %{
      settings: %{
        "index" => %{
          "analysis" => %{
            "analyzer" => %{
              "keyword_lowercase" => %{
                "type" => "custom",
                "tokenizer" => "keyword",
                "filter" => [
                  "lowercase"
                ]
              }
            }
          },
          "max_result_window" => "1000000",
          "refresh_interval" => "1s",
          "number_of_shards" => "1",
          "number_of_replicas" => "0"
        }
      },
      mappings: "priv/elasticsearch/event-mappings.json"
    }
  ],
  service: CogyntElasticsearch

config :pinot, :common,
  controller_url: "http://localhost:9000",
  broker_url: "http://localhost:8099",
  kafka_brokers: "localhost:9071",
  schema_registry_url: "http://localhost:8081",
  controller_service: Pinot.Controller,
  broker_service: Pinot.Broker

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.Cluster,
  event_index_alias: "event",
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
config :redis, :common,
  host: "127.0.0.1",
  port: 6379,
  sentinel_group: "main",
  pools: 5,
  exit_on_disconnection: false,
  sync_connect: true,
  instance: :single,
  client: Redis

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

# Broadway Pipelines configurations
config :cogynt_workstation_ingest, :event_pipeline,
  # 1 mib
  max_bytes: 1_048_576,
  batch_size: 100,
  # 10 sec
  batch_timeout: 10000

config :cogynt_workstation_ingest, :deployment_pipeline,
  processor_stages: 10,
  producer_stages: 5

# Configurations for keys in Cogynt Core events
config :cogynt_workstation_ingest, :cogynt_keys,
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
  source: "COG_source",
  data_type: "COG_data_type"

config :cogynt_workstation_ingest, :failed_messages,
  retry_timer: 300_000,
  max_retry: 5

config :cogynt_workstation_ingest, :consumer_retry_worker, retry_timer: 30_000

config :cogynt_workstation_ingest, :ingest_task_worker, timer: 1000

# RPC configurations
config :cogynt_workstation_ingest, :rpc,
  cogynt_auth_service_name: "http://localhost",
  cogynt_auth_service_port: 4999

config :cogynt_graphql, :common,
  mock_license: true,
  mock_license_status: "licensed",
  license_redirect_url: "http://localhost:3001/license"

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

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"
