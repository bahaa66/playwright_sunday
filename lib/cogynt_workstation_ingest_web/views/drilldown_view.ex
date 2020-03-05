defmodule CogyntWorkstationIngestWeb.DrilldownView do
  use CogyntWorkstationIngestWeb, :view
  use JaSerializer.PhoenixView
  alias CogyntWorkstationIngestWeb.JA_Keys
  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstatingIngest.EventView

  def type, do: "solutions"

  def id info, _conn do
    info["id"]
  end

  def attributes info, _conn do
    info
    |> Map.delete("id")
    |> Map.delete(:id)
    |> Map.delete("events")
    |> JA_Keys.dasherize()
  end

  has_many :children,
    serializer: __MODULE__,
    include: true,
    identifiers: :always

  def children info, _conn do
    (info["events"] || [])
    |> Enum.map(fn occ ->
      id = occ["published_by"]
      if id do
        {:ok, inst} = DrilldownCache.get(id)
        inst
      else
        nil
      end
    end)
    |> Enum.filter(& &1 != nil)
    |> Enum.uniq
    |> Enum.map(fn inst ->
      # Convert id to a unique id combining parent and child ids
      # put original id in "key", only if the parent has key.  This
      # ensures drill down has unique ids for all nodes, but index and
      # show use the real id.
      if info["key"] do
        Map.put(inst, "key", inst["id"])
        |> Map.put("id", info["id"]<>"|"<>inst["id"])
      else
        inst
      end
    end)
    |> Enum.sort_by(& &1["id"])
  end

  has_many :events,
    serializer: EventView,
    include: true,
    identifiers: :always

  def events info, _conn do
    info["events"]
    |> Enum.sort_by(& &1["id"])
  end
end
