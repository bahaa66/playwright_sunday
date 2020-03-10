use Mix.Config

config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
  http: [:inet6, port: System.get_env("PORT") || 4002],
  url: [host: "example.com", port: 80],
  cache_static_manifest: "priv/static/cache_manifest.json"

# Do not print debug messages in production
config :logger, level: :info

import_config "prod.secret.exs"
