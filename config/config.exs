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

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
