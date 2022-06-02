import Config

config :cogynt, CogyntWorkstationIngest.Repo, pool_size: 10

# CogyntWorkstationIngestWeb.Endpoint configurations
config :cogynt, CogyntWorkstationIngestWeb.Endpoint,
  debug_errors: false,
  code_reloader: false

config :elasticsearch, :common,
  indices: [
    event: %{
      settings: %{
        "index" => %{
          "analysis" => %{
            "analyzer" => %{
              "keyword_lowercase" => %{
                "type" => "custom",
                "tokenizer" => "keyword",
                "filter" => [
                  "lowercase"
                ]
              }
            }
          },
          "max_result_window" => "1000000",
          "refresh_interval" => "1s",
          "number_of_shards" => "3",
          "number_of_replicas" => "2"
        }
      },
      mappings: "priv/elasticsearch/event-mappings.json"
    }
  ]
