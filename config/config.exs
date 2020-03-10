use Mix.Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo]

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "jRj911i37hq28AsD+qzFkym2bm8tqo4xcZBMgf2p/OCHoFusw9VniPnh5N4BtvaZ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub: [name: CogyntWorkstationIngest.PubSub, adapter: Phoenix.PubSub.PG2]

# rpc server/client configurations
config :cogynt_workstation_ingest, :rpc,
  server_port: 80,
  cogynt_otp_url: "http://localhost:81/"

# cogynt-common configurations
config :migrations, :application,
  repo: CogyntWorkstationIngest.Repo

# Configures Elixir's Logger
# config :logger, :console,
#   format: "$time $metadata[$level] $message\n",
#   metadata: [:request_id]
config :logger, :console, format: "[$level] $message\n", level: :warn

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :phoenix, :format_encoders, "json-api": Jason

config :plug, :types, %{
  "application/vnd.api+json" => ["json-api"]
}

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
