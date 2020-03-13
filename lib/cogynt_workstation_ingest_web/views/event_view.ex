defmodule CogyntWorkstationIngestWeb.EventView do
  use CogyntWorkstationIngestWeb, :view
  alias CogyntWorkstationIngestWeb.JA_Keys

  use JaSerializer.PhoenixView

  @whitelist [:source, :"published-by", :"publishing-template-type",
    :"publishing-template-type-name", :"published-at", :"processed-at",
    :"source-type", :"data-type", :"assertion-id"]

  def type, do: "events"

  def id evt, _conn do
    evt["id"]
  end

  def attributes evt, _conn do
    data = evt
    |> Map.delete("id")
    |> JA_Keys.dasherize()
    data
    |> Enum.filter(fn {k,_v} ->
      Enum.member?(@whitelist, k)
    end)
    |> Enum.into(%{})
    |> Map.put(:fields, data
      |> Enum.reject(fn {k,_v} ->
        Enum.member?(@whitelist, k)
      end)
      |> Enum.into(%{}))
  end
end
