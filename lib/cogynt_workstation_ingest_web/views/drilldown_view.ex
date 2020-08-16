defmodule CogyntWorkstationIngestWeb.DrilldownView do
  use CogyntWorkstationIngestWeb, :view
  use JaSerializer.PhoenixView
  alias CogyntWorkstationIngestWeb.JA_Keys
  alias CogyntWorkstationIngestWeb.EventView
  # alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache

  def render("401.json-api", _) do
    %{errors: %{detail: "User is not authenticated"}}
  end

  def type, do: "solutions"

  def id(info, _conn) do
    info.id
  end

  def attributes(info, _conn) do
    info
    |> Map.delete(:id)
    |> Map.delete(:events)
    |> Map.delete(:outcomes)
    |> Map.delete("#visited")
    |> JA_Keys.dasherize()
  end

  has_many(:children,
    serializer: __MODULE__,
    include: true,
    identifiers: :always
  )

  def children(info, _conn) do
    (Map.values(info.events) || [])
    |> Enum.filter(
      &(not (Map.get(&1, :"$partial", false) == true and Map.get(&1, :_confidence, nil) == 0.0))
    )
    |> Enum.map(fn occ ->
      id = Map.get(occ, :published_by, false)

      if id do
        # {:ok, inst} = DrilldownContext.get_template_solution_data(id)
        {:ok, inst} = DrilldownCache.get(id)

        # inst
        if info.id == Map.get(inst, :id, nil) or
             Enum.member?(info["#visited"], Map.get(inst, :id, nil)) do
          nil
        else
          inst
        end
      else
        nil
      end
    end)
    |> Enum.filter(&(&1 != nil))
    |> Enum.uniq()
    |> Enum.map(fn inst ->
      inst_id = Map.get(inst, :id, nil)
      visited = [inst_id | info["#visited"]]
      # Convert id to a unique id combining parent and child ids
      # put original id in "key", only if the parent has key.  This
      # ensures drill down has unique ids for all nodes, but index and
      # show use the real id.
      if info["key"] do
        inst
        |> Map.put("key", Map.get(inst, :id, nil))
        |> Map.put(
          :id,
          info.id <> "|" <> Map.get(inst, :id, nil) <> "|" <> (inst.assertion_id || "")
        )
      else
        inst
      end
      |> Map.put("#visited", visited)
    end)
    |> Enum.sort_by(& &1.id)
  end

  has_many(:outcomes,
    serializer: EventView,
    include: true,
    identifiers: :always
  )

  def outcomes(info, _conn) do
    info.outcomes
    |> Enum.sort_by(& &1.id)
  end

  has_many(:events,
    serializer: EventView,
    include: true,
    identifiers: :always
  )

  def events(info, _conn) do
    if is_map(info.events) do
      Map.values(info.events)
      |> Enum.filter(
        &(not (Map.get(&1, :"$partial", false) == true and Map.get(&1, :_confidence, nil) == 0.0))
      )
      |> Enum.sort_by(& &1.id)
    else
      info.events
      |> Enum.filter(
        &(not (Map.get(&1, :"$partial", false) == true and Map.get(&1, :_confidence, nil) == 0.0))
      )
      |> Enum.sort_by(& &1.id)
    end
  end
end
