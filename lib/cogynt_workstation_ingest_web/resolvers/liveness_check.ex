defmodule CogyntWorkstationIngestWeb.Resolvers.LivenessCheck do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.ElasticsearchAPI

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
    {_, event_index_health} = ElasticsearchAPI.index_health?(Config.event_index_alias())
    {:ok, event_index_health}
  end
end
