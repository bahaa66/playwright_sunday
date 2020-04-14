use Mix.Config

config :cogynt_workstation_ingest,
  ecto_repos: [CogyntWorkstationIngest.Repo]

# Configures the endpoint
config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  url: [host: "localhost"],
  secret_key_base: "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ",
  render_errors: [view: CogyntWorkstationIngestWeb.ErrorView, accepts: ~w(json)],
  pubsub: [name: CogyntWorkstationIngest.PubSub, adapter: Phoenix.PubSub.PG2]

# cogynt-common configurations
config :migrations, :application, repo: CogyntWorkstationIngest.Repo

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

# Configures Elixir's Logger
config :logger, :console,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Use Jason for JSON parsing in Phoenix
config :phoenix, :json_library, Jason

config :phoenix, :format_encoders, "json-api": Jason

config :plug, :types, %{
  "application/vnd.api+json" => ["json-api"]
}

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{Mix.env()}.exs"
