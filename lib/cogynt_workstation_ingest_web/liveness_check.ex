defmodule LivenessCheck do
  import Plug.Conn
  alias CogyntWorkstationIngest.Config
  alias CogyntElasticsearch.Config, as: ElasticConfig

  @type options :: [resp_body: String.t()]

  @resp_body "Server's Up!"
  @resp_body_error "Internal server error"

  @spec init(options) :: options
  def init(opts \\ []) do
    [resp_body: opts[:resp_body] || @resp_body]
  end

  @spec call(Plug.Conn.t(), options) :: Plug.Conn.t()
  def call(%Plug.Conn{} = conn, _opts) do
    if kafka_health?() and postgres_health?() and redis_health?() and elastic_cluster_health?() do
      send_resp(conn, 200, @resp_body)
    else
      send_resp(conn, 500, @resp_body_error)
    end
  end

  defp kafka_health?() do
    try do
      case Kafka.Api.Topic.list_topics() do
        {:ok, _result} ->
          true

        _ ->
          CogyntLogger.error("#{__MODULE__}", "LivenessCheck Kafka Failed")
          false
      end
    rescue
      _ ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Kafka Failed")
        false
    end
  end

  defp postgres_health?() do
    try do
      query = "SELECT tablename FROM pg_tables
      WHERE tableowner = #{Config.postgres_username()} AND schemaname = 'public' AND tablename IN (
        'collection_items',
        'event_definition_details',
        'event_detail_template_group_items',
        'event_detail_template_groups',
        'event_detail_templates',
        'event_details',
        'event_links',
        'notification_system_tags',
        'notifications',
        'notification_settings',
        'events',
        'event_definitions'
      );"

      Ecto.Adapters.SQL.query(CogyntWorkstationIngest.Repo, query, [])
      true
    rescue
      DBConnection.ConnectionError ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "LivenessCheck PostgreSQL Failed with DBConnection.ConnectionError"
        )

        false
    end
  end

  defp redis_health?() do
    case Redis.ping() do
      {:ok, _pong} ->
        true

      {:error, :redis_connection_error} ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Failed on Redis Connection Error")
        false

      _ ->
        CogyntLogger.error("#{__MODULE__}", "LivenessCheck Failed on Redis Internal Server Error")
        false
    end
  end

  defp elastic_cluster_health?() do
    ElasticConfig.elasticsearch_service().get_cluster_health()
    |> case do
      {:ok, %{"status" => status}} when status in ["green", "yellow"] ->
        true

      {:ok, res} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Unexpected LivenessCheck response for Elastic Cluster. Response: #{inspect(res)}"
        )

        false

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "LivenessCheck for Elastic Cluster failed. Error: #{inspect(error)}"
        )

        false
    end
  end
end
