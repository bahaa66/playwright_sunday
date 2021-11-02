use Mix.Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo]

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub_server: CogyntWorkstationIngestWeb.PubSub

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
  utc_offset: 0

# Redis Configurations
config :redis, :application, port: 6379

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
import_config "#{Mix.env()}.exs"
