defmodule CogyntWorkstationIngestWeb.Clients.JsonRpcHTTPClient do
  @behaviour CogyntWorkstationIngestWeb.Clients.JsonRpcHTTPClient

  @default_headers [{"content-type", "application/json"}]
  def call(
        url,
        method,
        params,
        headers \\ @default_headers,
        http_method \\ :post,
        hackney_opts \\ [],
        request_id \\ "0"
      ) do
    JSONRPC2.Clients.HTTP.call(
      url,
      method,
      params,
      headers,
      http_method,
      hackney_opts,
      request_id
    )
  end
end
