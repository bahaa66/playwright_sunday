defmodule LivenessCheck do
  import Plug.Conn
  alias CogyntWorkstationIngest.Config

  @path "/_cluster/health?wait_for_status=green&timeout=10s"
  @type options :: [resp_body: String.t()]

  @resp_body "Server's Up!"
  @resp_body_error "Internal server error"

  @spec init(options) :: options
  def init(opts \\ []) do
    [resp_body: opts[:resp_body] || @resp_body]
  end

  @spec call(Plug.Conn.t(), options) :: Plug.Conn.t()
  def call(%Plug.Conn{} = conn, _opts) do
    if elasticsearch_health?() and kafka_health?() and postgres_health?() and redis_health?() do
      send_resp(conn, 200, @resp_body)
    else
      send_resp(conn, 500, @resp_body_error)
    end
  end

  defp elasticsearch_health?() do
    elastic_health_url = "#{Config.elasticsearch_host()}#{@path}"

    case HTTPoison.get(elastic_health_url) do
      {:ok, %HTTPoison.Response{status_code: 200, body: _body}} ->
        true

      {:ok, %HTTPoison.Response{status_code: 404}} ->
        false

      {:error, _error} ->
        false
    end
  end

  defp kafka_health?() do
    try do
      _result = KafkaEx.metadata(worker_name: :drilldown)
      true
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
