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

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.EventPipeline,
  processor_stages: System.get_env("EVENTPIPELINE_PROCESSOR_STAGES") || 22,
  processor_max_demand: System.get_env("EVENTPIPELINE_PROCESSOR_MAX_DEMAND") || 10_000,
  processor_min_demand: System.get_env("EVENTPIPELINE_PROCESSOR_MIN_DEMAND") || 5_000

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Broadway.LinkEventPipeline,
  processor_stages: System.get_env("LINKEVENTPIPELINE_PROCESSOR_STAGES") || 10,
  processor_max_demand: System.get_env("LINKEVENTPIPELINE_PROCESSOR_MAX_DEMAND") || 10_000,
  processor_min_demand: System.get_env("LINKEVENTPIPELINE_PROCESSOR_MIN_DEMAND") || 5_000

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

# EventDocument Configurations
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.EventDocument,
  index_alias: System.get_env("EVENT_INDEX_ALIAS") || "event"

# ConsumerRetryCache Configurations
config :cogynt_workstation_ingest, CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache,
  time_delay: System.get_env("CONSUMER_RETRY_CACHE_TIME_DELAY") || 600_000,
  max_retry: System.get_env("CONSUMER_RETRY_CACHE_MAX_RETRY") || 144

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Servers.Caches.DrilldownCache,
  time_delay: System.get_env("DRILLDOWN_CACHE_TIME_DELAY") || 1_000

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

# Import Common Library Config
import_config "common.exs"
