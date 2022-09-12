defmodule CogyntWorkstationIngest.Pinot.Controller do
  use Tesla
  use CogyntWorkstationIngest.Pinot

  # TODO: Make this configurable
  plug Tesla.Middleware.BaseUrl, "https://pinot-dev1.cogilitycloud.com"
  plug Tesla.Middleware.JSON, engine_opts: [keys: :atoms]

  @callback get_health() :: {:ok, String.t()} | api_error
  def get_health() do
    get("/health")
    |> handle_response()
  end

  @callback get_schemas() :: {:ok, list(map)} | api_error()
  def get_schemas() do
    get("/schemas")
    |> handle_response()
  end

  @callback get_schema(name :: String.t()) :: {:ok, map} | api_error()
  def get_schema(name) when is_binary(name) do
    get("/schemas/#{name}")
    |> handle_response()
  end

  @type schema_input :: %{
          required(:schemaName) => map(),
          required(:dimensionFieldSpecs) => list(map()),
          required(:dateTimeFieldSpecs) => list(map())
        }
  @callback validate_schema(schema :: schema_input()) :: {:ok, map} | api_error()
  def validate_schema(schema) do
    post("/schemas/validate", schema)
    |> handle_response()
  end

  @callback validate_schema(
              schema :: schema_input(),
              opts :: [query: [override: boolean()]] | nil
            ) :: {:ok, map} | api_error()
  def create_schema(schema, opts \\ []) do
    post("/schemas", schema, opts)
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
  @type tables_response :: %{tables: list(map())}
  @callback get_tables(opts :: [query: get_tables_query_params()] | nil) ::
              {:ok, tables_response()} | api_error
  def get_tables(opts \\ []) do
    get("/tables", opts)
    |> handle_response()
  end

  @callback get_table(name :: String.t(), opts: [] | nil) :: {:ok, map()} | api_error
  def get_table(name, opts \\ []) do
    get("/tables/#{name}", opts)
    |> handle_response()
  end

  @callback validate_table(table :: map()) :: {:ok, map()} | api_error
  def validate_table(table) do
    post("/tables/validate", table)
    |> handle_response()
  end

  @callback create_table(table :: map()) :: {:ok, map()} | api_error
  def create_table(table) do
    post("/tables", table)
    |> handle_response()
  end

  @callback update_table(table_name :: String.t(), table :: map()) :: {:ok, map()} | api_error
  def update_table(table_name, table) do
    put("/tables/#{table_name}", table)
    |> handle_response()
  end
end
