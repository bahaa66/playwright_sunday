defmodule LivenessCheck do
  import Plug.Conn
  alias CogyntWorkstationIngest.Config

  @type options :: [resp_body: String.t()]

  @resp_body "Server's Up!"
  @resp_body_error "Internal server error"

  @spec init(options) :: options
  def init(opts \\ []) do
    [resp_body: opts[:resp_body] || @resp_body]
  end

  @spec call(Plug.Conn.t(), options) :: Plug.Conn.t()
  def call(%Plug.Conn{} = conn, _opts) do
    {_, cluster_health} = Elasticsearch.cluster_health?()
    {_, event_index_health} = Elasticsearch.index_health?(Config.event_index_alias())

    {_, risk_history_index_health} =
      Elasticsearch.index_health?(Config.risk_history_index_alias())

    if cluster_health and kafka_health?() and postgres_health?() and redis_health?() and
      event_index_health and risk_history_index_health do
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
          false
      end
    rescue
      _ ->
        false
    end
  end

  defp postgres_health?() do
    try do
      Ecto.Adapters.SQL.query(CogyntWorkstationIngest.Repo, "select 1", [])
      true
    rescue
      DBConnection.ConnectionError -> false
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
end
