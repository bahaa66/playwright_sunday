import Config

config :cogynt_workstation_ingest, CogyntWorkstationIngest.Repo, pool_size: 20

# Set a higher stacktrace during development. Avoid configuring such
# in production as building large stacktraces may be expensive.
config :phoenix, :stacktrace_depth, 20

# Initialize plugs at runtime for faster development compilation
config :phoenix, :plug_init_mode, :runtime

config :libcluster,
  topologies: [
    k8s_ws_ingest: [
      strategy: Elixir.Cluster.Strategy.Kubernetes,
      config: [
        mode: :hostname,
        kubernetes_service_name: "ws-ingest-otp",
        kubernetes_node_basename: "ws-ingest-otp",
        kubernetes_selector: "k8s.cogynt.io/name=ws-ingest-otp",
        kubernetes_namespace: System.get_env("NAMESPACE") || "cogynt-kots",
        polling_interval: 10_000
      ]
    ]
  ]
