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
  brokers: [
    {
      System.get_env("KAFKA_BROKER") || "172.16.1.100",
      System.get_env("KAFKA_PORT") || 9092
    }
  ],
  # Local Kafka
  # brokers: [{"127.0.0.1", 9092}],
  auto_offset_reset: :earliest,
  kafka_version: "2.0",
  commit_interval: System.get_env("KAFKA_COMMIT_INTERVAL") || 1000,
  commit_threshold: System.get_env("KAFKA_COMMIT_THRESHOLD") || 50,
  heartbeat_interval: System.get_env("KAFKA_HEARTBEAT_INTERVAL") || 1000,
  kafka_client: System.get_env("KAFKA_CLIENT") || KafkaEx,
  audit_topic: System.get_env("AUDIT_LOG_TOPIC") || "cogynt_audit_log"

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
