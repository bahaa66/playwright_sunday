defmodule CogyntWorkstationIngestWeb.DrilldownView do
  use CogyntWorkstationIngestWeb, :view
  use JaSerializer.PhoenixView
  alias CogyntWorkstationIngestWeb.JA_Keys
  # alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngestWeb.EventView

  def render("401.json-api", _) do
    %{errors: %{detail: "User is not authenticated"}}
  end

  def type, do: "solutions"

  def id(info, _conn) do
    info["id"]
  end

  def attributes(info, _conn) do
    info
    |> Map.delete("id")
    |> Map.delete(:id)
    |> Map.delete("events")
    |> Map.delete("outcomes")
    |> Map.delete("#visited")
    |> JA_Keys.dasherize()
  end

  has_many(:children,
    serializer: __MODULE__,
    include: true,
    identifiers: :always
  )

  def children(info, _conn) do
    Map.values(info["events"])
    |> Enum.filter(&(not (&1["$partial"] == true and &1["_confidence"] == 0.0)))
    |> Enum.map(fn occ ->
      id = occ["published_by"]

      if id do
        # {:ok, inst} = DrilldownCache.get(id)
        inst = DrilldownContext.get_template_solution_data(id)
        # inst
        if info["id"] == inst["id"] or Enum.member?(info["#visited"], inst["id"]) do
          # IO.inspect(occ, label: "@@@@ Event published by self?")
          # IO.inspect(inst, label: "@@@@ Instance causing recursion")
          nil
        else
          # CogyntLogger.info(
          #   "Drilldown View",
          #   "@@@@ Instance #{inst["id"]} parent #{info["key"]}"
          # )

          inst
        end
      else
        nil
      end
    end)
    |> Enum.filter(&(&1 != nil))
    |> Enum.uniq()
    |> Enum.map(fn inst ->
      inst_id = inst["id"]
      visited = [inst_id | info["#visited"]]
      # Convert id to a unique id combining parent and child ids
      # put original id in "key", only if the parent has key.  This
      # ensures drill down has unique ids for all nodes, but index and
      # show use the real id.
      if info["key"] do
        inst
        |> Map.put("key", inst["id"])
        |> Map.put("id", info["id"] <> "|" <> inst["id"] <> "|" <> (inst["assertion_id"] || ""))
      else
        inst
      end
      |> Map.put("#visited", visited)
    end)
    |> Enum.sort_by(& &1["id"])
  end

  has_many(:outcomes,
    serializer: EventView,
    include: true,
    identifiers: :always
  )

  def outcomes(info, _conn) do
    info["outcomes"]
    |> Enum.sort_by(& &1["id"])
  end

  has_many(:events,
    serializer: EventView,
    include: true,
    identifiers: :always
  )

  def events(info, _conn) do
    if is_map(info["events"]) do
      Map.values(info["events"])
      |> Enum.filter(&(not (&1["$partial"] == true and &1["_confidence"] == 0.0)))
      |> Enum.sort_by(& &1["id"])
    else
      info["events"]
      |> Enum.filter(&(not (&1["$partial"] == true and &1["_confidence"] == 0.0)))
      |> Enum.sort_by(& &1["id"])
    end
  end
end
