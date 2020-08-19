defmodule LivenessCheck do
  import Plug.Conn
  alias CogyntWorkstationIngest.Config

  @path "/_cluster/health?wait_for_status=green&timeout=10s"
  @index_path "/_cluster/health/"
  @index_params "?wait_for_status=green&timeout=10s"
  @type options :: [resp_body: String.t()]

  @resp_body "Server's Up!"
  @resp_body_error "Internal server error"

  @spec init(options) :: options
  def init(opts \\ []) do
    [resp_body: opts[:resp_body] || @resp_body]
  end

  @spec call(Plug.Conn.t(), options) :: Plug.Conn.t()
  def call(%Plug.Conn{} = conn, _opts) do
    if elasticsearch_health?() and kafka_health?() and postgres_health?() and redis_health?() and
         elasticsearch_index_health?(Config.event_index_alias()) and
         elasticsearch_index_health?(Config.risk_history_index_alias()) do
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

  defp elasticsearch_index_health?(index) do
    elastic_health_url = "#{Config.elasticsearch_host()}#{@index_path}#{index}#{@index_params}"

    case HTTPoison.get(elastic_health_url) do
      {:ok, %HTTPoison.Response{status_code: 200, body: _body}} ->
        true

      {:ok, %HTTPoison.Response{status_code: 404}} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Elasticsearch Healthcheck failed with status_code 404 response"
        )

        false

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Elasticsearch Healthcheck failed with Error: #{inspect(error)}"
        )

        false
    end
  end

  defp kafka_health?() do
    try do
      _worker_result =
        KafkaEx.create_worker(:livenessCheckWorker,
          consumer_group: "kafka_ex",
          consumer_group_update_interval: 100
        )

      _metadata_result = KafkaEx.metadata(worker_name: :livenessCheckWorker)
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
