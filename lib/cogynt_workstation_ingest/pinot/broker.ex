defmodule CogyntWorkstationIngest.Pinot.Broker do
  use Tesla
  use CogyntWorkstationIngest.Pinot

  # TODO: Make this configurable
  plug Tesla.Middleware.BaseUrl, "https://pinot-broker-dev1.cogilitycloud.com:443"
  plug Tesla.Middleware.JSON, engine_opts: [keys: :atoms]
  plug Tesla.Middleware.Logger, format: "$method $url ====> $status / time=$time"

  @type pinot_sql_query :: %{required(:sql) => String.t()}
  @callback query(query :: pinot_sql_query()) :: {:ok, map()} | api_error()
  def query(query) do
    post("/query/sql", query)
    |> handle_response()
    |> handle_query_response()
  end

  defp handle_query_response({:ok, %{resultTable: result_table}}) do
    column_names = get_in(result_table, [:dataSchema, :columnNames])

    rows =
      Enum.map(result_table[:rows], fn row ->
        Enum.with_index(row, fn value, index ->
          {Enum.at(column_names, index), value}
        end)
        |> Enum.into(%{})
      end)

    {:ok, rows}
  end

  defp handle_query_response(res), do: res
end
