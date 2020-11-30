use Mix.Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo],
  enable_dev_tools: (System.get_env("ENABLE_DEV_TOOLS") || "true") == "true"

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub_server: CogyntWorkstationIngestWeb.PubSub

# cogynt-common configurations
config :migrations, :application, repo: CogyntWorkstationIngest.Repo

config :cogynt_workstation_ingest, :clients,
  http_client: HTTPoison,
  elasticsearch_client: Elasticsearch

# Kafka Configurations
config :kafka, :application,
  kafka_client: :brod,
  template_solutions_topic: "template_solutions",
  template_solution_events_topic: "template_solution_events",
  deployment_topic: "deployment"

# Elasticsearch Configurations
config :elasticsearch, :application,
  elasticsearch_client: Elasticsearch,
  http_client: HTTPoison,
  event_index_alias: "event",
  event_index_alias: "event",
  risk_history_index_alias: "risk_history",
  retry_on_conflict: 5,
  utc_offset: 0

# Redis Configurations
config :redis, :application, port: 6379

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
  scheduler_enable: false,
  max_retries: 25,
  mode: :default,
  shutdown_timeout: 15000,
  heartbeat_enable: true,
  heartbeat_interval: 60_000,
  missed_heartbeats_allowed: 5

# Configurations for keys in Cogynt Core events
config :cogynt_workstation_ingest, :core_keys,
  crud: "$crud",
  risk_score: "_confidence",
  partial: "$partial",
  events: "$$events",
  description: "$description",
  entities: "$$entities",
  lexicons: "$matches",
  link_data_type: "linkage",
  update: "update",
  delete: "delete",
  create: "create"

config :cogynt_workstation_ingest, :drilldown_pipeline, enabled: true

config :cogynt_workstation_ingest, :failed_messages,
  retry_timer: 600_000,
  max_retry: 144

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

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
