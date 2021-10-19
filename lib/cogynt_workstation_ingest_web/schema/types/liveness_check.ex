defmodule CogyntWorkstationIngestWeb.Schema.Types.LivenessCheck do
  use Absinthe.Schema.Notation
  alias CogyntWorkstationIngestWeb.Resolvers.LivenessCheck, as: LivenessCheckResolver

  object :liveness_check_queries do
    field :liveness_check, non_null(:workstation_ingest_liveness_check) do
      resolve(fn _, _, _ ->
        {:ok, %{}}
      end)
    end
  end

  object :workstation_ingest_liveness_check do
    field :redis_health, :boolean, do: resolve(&LivenessCheckResolver.redis_healthy?/3)
    field :postgres_health, :boolean, do: resolve(&LivenessCheckResolver.postgres_healthy?/3)
    field :kafak_health, :boolean, do: resolve(&LivenessCheckResolver.kafka_healthy?/3)

    field :elasticsearch_health, :boolean,
      do: resolve(&LivenessCheckResolver.elasticsearch_healthy?/3)
  end
end
