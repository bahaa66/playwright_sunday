# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
use Mix.Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo]

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "jRj911i37hq28AsD+qzFkym2bm8tqo4xcZBMgf2p/OCHoFusw9VniPnh5N4BtvaZ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub: [name: CogyntWorkstationIngest.PubSub, adapter: Phoenix.PubSub.PG2]

# Kafka Configurations
config :kafka_ex,
  # Dev Kafka
  # brokers: [
  #   {
  #     System.get_env("KAFKA_BROKER") || "172.16.1.100",
  #     System.get_env("KAFKA_PORT") || 9092
  #   }
  # ],
  # Local Kafka
  brokers: [{"127.0.0.1", 9092}],
  auto_offset_reset: :earliest,
  kafka_version: "2.0",
  commit_interval: System.get_env("KAFKA_COMMIT_INTERVAL") || 1000,
  commit_threshold: System.get_env("KAFKA_COMMIT_THRESHOLD") || 50,
  heartbeat_interval: System.get_env("KAFKA_HEARTBEAT_INTERVAL") || 1000,
  kafka_client: System.get_env("KAFKA_CLIENT") || KafkaEx,
  audit_topic: System.get_env("AUDIT_LOG_TOPIC") || "cogynt_audit_log"

# Elasticsearch configurations
config :elasticsearch, :config,
  enabled: System.get_env("ELASTICSEARCH_ENABLED") || true,
  basic_authentication_enabled: true,
  host: System.get_env("ELASTIC_URL") || "http://localhost:9200",
  username: System.get_env("ELASTIC_USERNAME") || "elasticsearch",
  password: System.get_env("ELASTIC_PASSWORD") || "elasticsearch",
  elasticsearch_client: System.get_env("ELASTIC_CLIENT") || Elasticsearch,
  utc_offset: 0

# Configurations for keys in Cogynt Core events
config :cogynt_workstation_ingest, :core_keys,
  crud: System.get_env("CORE_KEYS_CRUD") || "$crud",
  risk_score: System.get_env("CORE_KEYS_RISK_SCORE") || "_confidence",
  partial: System.get_env("CORE_KEYS_PARTIAL") || "$partial",
  events: System.get_env("CORE_KEYS_EVENTS") || "$$events",
  description: System.get_env("CORE_KEYS_DESCRIPTION") || "$description",
  entities: System.get_env("CORE_KEYS_ENTITIES") || "$$entities",
  link_data_type: System.get_env("CORE_KEYS_LINK_DATA_TYPE") || :linkage,
  update: System.get_env("CORE_KEYS_UPDATE") || "update",
  delete: System.get_env("CORE_KEYS_DELETE") || "delete",
  create: System.get_env("CORE_KEYS_CREATE") || "create"

# EventDocument Configurations
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.EventDocument,
  index_alias: System.get_env("EVENT_INDEX_ALIAS") || "event"

# Configures Elixir's Logger
# config :logger, :console,
#   format: "$time $metadata[$level] $message\n",
#   metadata: [:request_id]
config :logger, :console, format: "[$level] $message\n", level: :warn

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
