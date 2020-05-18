defmodule CogyntWorkstationIngestWeb.EventView do
  use CogyntWorkstationIngestWeb, :view
  alias CogyntWorkstationIngestWeb.JA_Keys

  use JaSerializer.PhoenixView

  @whitelist [
    "source",
    "published_by",
    "publishing_template_type",
    "publishing_template_type_name",
    "published_at",
    "processed_at",
    "source_type",
    "data_type",
    "assertion_id"
  ]

  def type, do: "events"

  def id(evt, _conn) do
    evt["id"]
  end

  def attributes(evt, _conn) do
    attrs =
      evt
      |> Enum.filter(fn {k, _v} ->
        Enum.member?(@whitelist, k)
      end)
      |> Enum.into(%{})
      |> JA_Keys.dasherize()

    fields =
      evt
      |> Enum.reject(fn {k, _v} ->
        Enum.member?(@whitelist, k)
      end)
      |> Enum.into(%{})
      |> Map.delete("id")

    attrs
    |> Map.put("fields", fields)
  end
end
