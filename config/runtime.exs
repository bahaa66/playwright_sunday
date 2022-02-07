import Config

config :cogynt_workstation_ingest, :environment, config_env()

cond do
  # Configs that are applied at runtime for our server environments.
  config_env() in [:prod, :k8scyn] ->
    signing_salt = System.get_env("COGYNT_AUTH_SALT", "I45Kpw9a")

    # Session Configurations
    config :cogynt_workstation_ingest,
      session_domain: System.get_env("COGYNT_SESSION_DOMAIN", "localhost"),
      # TODO: Suggestion. Make this env specific so we can log into different envs without conflicting? "_cogynt_auth_key_#{config_env() |> Atom.to_string()}"
      session_key: System.get_env("COGYNT_AUTH_SESSION_KEY", "_cogynt_auth_key"),
      signing_salt: signing_salt,
      enable_dev_tools: (System.get_env("ENABLE_DEV_TOOLS") || "true") == "true",
      authoring_version: System.get_env("COGYNT_AUTHORING_VERSION", "1")

    config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
      url: [host: System.get_env("COGYNT_DOMAIN", "localhost")],
      secret_key_base: System.get_env("COGYNT_SECRET_KEY_BASE", "YqoQsxs2MpNBdH4PrtQYNY1JnJfscSFBIADEDqs6wSMIn3/8+TjYkbm6CrPx2yVJ"),
      https: [
        port: System.get_env("HTTPS_PORT", "450") |> String.to_integer(),
        otp_app: :cogynt_workstation_ingest,
        keyfile: System.get_env("TLS_KEY_PATH"),
        certfile: System.get_env("TLS_CERT_PATH")
      ],
      check_origin: false,
      live_view: [signing_salt: signing_salt]

    # Configure your database
    config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo,
      username: System.get_env("POSTGRESQL_USERNAME"),
      password: System.get_env("POSTGRESQL_PASSWORD"),
      database: System.get_env("POSTGRESQL_DATABASE"),
      hostname: System.get_env("POSTGRESQL_HOST"),
      pool_size: System.get_env("POSTGRESQL_POOL_SIZE", "20") |> String.to_integer(),
      telemetry_prefix: [:cogynt_workstation_ingest, :repo]

    # Kafka Configurations
    config :kafka, :application,
      brokers: System.get_env("KAFKA_BROKERS"),
      partition_strategy: System.get_env("PARTITION_STRATEGY", "random") |> String.to_atom(),
      partitions: System.get_env("PARTITIONS", "10") |> String.to_integer(),
      replication_factor:
        System.get_env("KAFKA_TOPIC_REPLICATION_FACTOR", "1") |> String.to_integer(),
      replica_assignment: System.get_env("REPLICA_ASSIGNMENT") || [],
      config_entries: System.get_env("CONFIG_ENTRIES") || [],
      session_timeout: System.get_env("SESSION_TIMEOUT", "10000") |> String.to_integer(),
      kafka_connect_host: System.get_env("KAFKA_CONNECT_URL") || "http://localhost:8083"

    # Elasticsearch configurations (ARE THESE STILL NEEDED?)
    config :elasticsearch, :application,
      cacertfile: System.get_env("ELASTIC_CA_CERT_PATH"),
      host: System.get_env("ELASTIC_URL"),
      username: System.get_env("ELASTIC_USERNAME"),
      password: System.get_env("ELASTIC_PASSWORD"),
      shards: System.get_env("ELASTIC_SHARDS"),
      replicas: System.get_env("ELASTIC_REPLICAS")

    config :cogynt_workstation_ingest, CogyntWorkstationIngest.Elasticsearch.Cluster,
      username: System.get_env("ELASTIC_USERNAME"),
      password: System.get_env("ELASTIC_PASSWORD"),
      url: System.get_env("ELASTIC_URL")

    config :exq,
      redis_options: [
        sentinel: [
          sentinels:
            String.split(System.get_env("COGYNT_REDIS_SENTINELS") || "", ",", trim: true),
          group: System.get_env("COGYNT_REDIS_SENTINEL_GROUP") || "main"
        ],
        name: Exq.Redis.Client,
        password: System.get_env("COGYNT_REDIS_PASSWORD") || nil
      ]

    config :redis, :application,
      host: System.get_env("COGYNT_REDIS_HOST"),
      password: System.get_env("COGYNT_REDIS_PASSWORD"),
      name: System.get_env("COGYNT_REDIS_NAME"),
      sentinels: System.get_env("COGYNT_REDIS_SENTINELS"),
      sentinel_group: System.get_env("COGYNT_REDIS_SENTINEL_GROUP"),
      database: System.get_env("COGYNT_REDIS_DATABASE"),
      pools: System.get_env("COGYNT_REDIS_POOLS"),
      exit_on_disconnection: (System.get_env("ENABLE_DEV_TOOLS") || "true") == "true",
      sync_connect: (System.get_env("ENABLE_DEV_TOOLS") || "true") == "true",
      instance: System.get_env("COGYNT_REDIS_INSTANCE", "single") |> String.to_atom()

    config :druid,
      request_timeout: System.get_env("DRUID_REQUEST_TIMEOUT", "120000") |> String.to_integer(),
      query_priority: System.get_env("DRUID_QUERY_PRIORITY", "0") |> String.to_integer(),
      broker_profiles: [
        default: [
          base_url: System.get_env("DRUID_BASE_URL"),
          cacertfile: System.get_env("DRUID_CERT_FILE_PATH", "path/to/druid-certificate.crt"),
          http_username: System.get_env("DRUID_HTTP_USERNAME", "username"),
          http_password: System.get_env("DRUID_HTTP_PASSWORD", "password")
        ]
      ],
      schema_registry_url: System.get_env("SCHEMA_REGISTRY_URL", "http://schemaregistry:8081")

    # Broadway Pipelines configurations
    config :cogynt_workstation_ingest, :event_pipeline,
      processor_stages:
        System.get_env("EVENTPIPELINE_PROCESSOR_STAGES", "10") |> String.to_integer(),
      producer_stages:
        System.get_env("EVENTPIPELINE_PRODUCER_STAGES", "10") |> String.to_integer()

    config :cogynt_workstation_ingest, :deployment_pipeline,
      processor_stages: System.get_env("DEPLOYMENT_PROCESSOR_STAGES", "2") |> String.to_integer(),
      producer_stages: System.get_env("DEPLOYMENT_PRODUCER_STAGES", "2") |> String.to_integer()

    # RPC configurations
    config :cogynt_workstation_ingest, :rpc,
      cogynt_auth_service_name: System.get_env("COGYNT_AUTH_SERVICE_NAME"),
      cogynt_auth_service_port:
        System.get_env("COGYNT_AUTH_SERVICE_PORT", "4999") |> String.to_integer()

  # Configs needed for local dev environments and test envs.
  config_env() not in [:dev, :test] ->
    config :cogynt_workstation_ingest, CogyntWorkstationIngestWeb.Endpoint,
      http: [port: (System.get_env("HTTP_PORT") || "4002") |> String.to_integer()]

    # Kafka Configurations
    config :kafka, :application,
      brokers: "127.0.0.1:9092",
      partition_strategy: :random,
      partitions: 10,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: [],
      session_timeout: 10000,
      kafka_connect_host: "http://localhost:8083"

    # Redis Configurations
    config :redis, :application,
      port: 6379,
      host: "127.0.0.1"

    config :exq,
      redis_options: [
        host: "127.0.0.1",
        port: 6379,
        name: Exq.Redis.Client,
        password: nil
      ]
end

if config_env() not in [:dev, :test, :k8scyn] do
  config :libcluster,
    topologies: [
      k8s_ws_ingest: [
        strategy: Elixir.Cluster.Strategy.Kubernetes.DNS,
        config: [
          service: System.get_env("SERVICE_NAME") || "ws-ingest-otp-headless",
          application_name: "ws-ingest-otp",
          polling_interval: 10_000
        ]
      ]
    ]
end
