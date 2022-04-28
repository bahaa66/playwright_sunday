import Config

config :cogynt, CogyntWorkstationIngest.Repo, pool_size: 10

# CogyntWeb.Endpoint configurations
config :cogynt, CogyntWorkstationIngestWeb.Endpoint,
  debug_errors: false,
  code_reloader: false

config :cogynt_graphql, :common, mock_license: false
