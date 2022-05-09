defmodule CogyntWorkstationIngestWeb.Resolvers.LivenessCheck do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi
  alias CogyntElasticsearch.Config, as: ElasticConfig

  def redis_healthy?(_, _, _) do
    case Redis.ping() do
      {:ok, _pong} ->
        {:ok, true}

      {:error, :redis_connection_error} ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Failed on Redis Connection Error")
        {:ok, false}

      _ ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Failed on Redis Internal Server Error")
        {:ok, false}
    end
  end

  def postgres_healthy?(_, _, _) do
    try do
      query = "SELECT tablename FROM pg_tables
      WHERE tableowner = #{Config.postgres_username()} AND schemaname = 'public' AND tablename IN (
        'collection_items',
        'colelction_system_tags',
        'deployments',
        'event_definition_details',
        'event_definitions',
        'event_detail_template_group_items',
        'event_detail_template_groups',
        'event_detail_templates',
        'event_links',
        'events',
        'notification_settings',
        'notification_system_tags',
        'notifications',
        'system_notification_configurations',
        'system_notification_types'
      );"

      Ecto.Adapters.SQL.query(CogyntWorkstationIngest.Repo, query, [])
      {:ok, true}
    rescue
      DBConnection.ConnectionError ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "LivenessCheck PostgreSQL Failed with DBConnection.ConnectionError"
        )

        {:ok, false}
    end
  end

  def kafka_healthy?(_, _, _) do
    try do
      case Kafka.Api.Topic.list_topics() do
        {:ok, _result} ->
          {:ok, true}

        _ ->
          CogyntLogger.error("#{__MODULE__}", "LivenessCheck Kafka Failed")
          {:ok, false}
      end
    rescue
      _ ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Kafka Failed")
        {:ok, false}
    end
  end

  def elasticsearch_healthy?(_, _, _) do
    {:ok, indices_healthy?()}
  end

  defp indices_healthy?() do
    # Get the indices from the configs
    ElasticConfig.elasticsearch_indices()
    # The keys are the aliases
    |> Keyword.keys()
    |> Enum.reduce_while(true, fn a, acc ->
      Atom.to_string(a)
      # Wait for the green status
      |> ElasticConfig.elasticsearch_service().get_index_health(
        query: [wait_for_status: "green", timeout: "10s"]
      )
      |> case do
        {:ok, %{"status" => "green"}} ->
          {:cont, acc && true}

        {:ok, res} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Uneexpected LivenessCheck response for #{inspect(a)} index. Response: #{inspect(res)}"
          )

          {:halt, false}

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "LivenessCheck for #{inspect(a)} index failed. Error: #{inspect(error)}"
          )

          {:halt, false}
      end
    end)
  end
end
