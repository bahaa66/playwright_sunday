defmodule CogyntWorkstationIngest.Pinot.Broker do
  use Tesla
  use CogyntWorkstationIngest.Pinot

  # TODO: Make this configurable
  plug Tesla.Middleware.BaseUrl, "https://127.0.0.1:8099"
  plug Tesla.Middleware.JSON, engine_opts: [keys: :atoms]
  plug Tesla.Middleware.Logger, format: "$method $url ====> $status / time=$time"

  def query(query) do
    post("/query/sql", query)
    |> handle_response()
  end
end
