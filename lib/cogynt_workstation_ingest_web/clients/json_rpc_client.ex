defmodule CogyntWorkstationIngestWeb.Clients.JsonRpcClient do
  @callback call(String.t(), JSONRPC2.method(), JSONRPC2.params(), any, atom, list, JSONRPC2.id()) ::
              {:ok, any} | {:error, any}
  @callback call(String.t(), JSONRPC2.method(), JSONRPC2.params()) :: {:ok, any} | {:error, any}
end
