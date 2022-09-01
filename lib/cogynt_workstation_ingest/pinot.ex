defmodule CogyntWorkstationIngest.Pinot do
  use Tesla

  # TODO: Make this configurable
  plug Tesla.Middleware.BaseUrl, "https://pinot-dev1.cogilitycloud.com"
  plug Tesla.Middleware.JSON

  @type api_error :: {:error, {integer(), map()}} | {:error, any()}

  @callback get_health() :: {:ok, String.t()} | api_error
  def get_health() do
    get("/health")
    |> handle_response()
  end

  @typedoc """
  "realtime" | "offline"
  """
  @type table_type :: String.t()
  @typedoc """
  "name" | "creationTime" | "lastModifiedTime"
  """
  @type sort_tabel :: String.t()
  @type get_tables_query_params :: [
          type: table_type(),
          sortType: sort_tabel(),
          sortAsc: boolean()
        ]
  @callback get_tables(opts :: [query: get_tables_query_params()]) :: {:ok, %{"tables" => [map()]}} | api_error
  def get_tables(opts \\ []) do
    get("/tables", opts)
    |> handle_response()
  end

  @spec handle_response(response :: Tesla.Env.result()) ::
          {:ok, any()} | {:error, {integer(), map()}} | {:error, any()}
  defp handle_response(response) do
    response
    |> case do
      {:ok, %Tesla.Env{body: %{"error" => error, "status" => status}}} ->
        {:error, {status, error}}

      {:ok, %Tesla.Env{body: body}} ->
        {:ok, body}

      {:error, error} ->
        {:error, error}
    end
  end
end
