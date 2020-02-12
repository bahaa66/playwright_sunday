defmodule CogyntWorkstationIngestWeb.Channels.IngestSocket do
  use Phoenix.Socket

  ## Channels
  channel "ingest:*", CogyntWorkstationIngestWeb.ConsumerChannel

  def connect(_params, socket, _connect_info) do
    {:ok, socket}
  end

  def id(_socket), do: nil
end
