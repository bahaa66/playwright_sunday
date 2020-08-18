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
        case DrilldownCache.get(id) do
          {:ok, nil} ->
            nil

          {:ok, inst} ->
            # inst
            if info.id == inst.id or Enum.member?(info["#visited"], inst.id) do
              nil
            else
              inst
            end
        end
      else
        nil
      end
    end)
    |> Enum.filter(&(&1 != nil))
    |> Enum.uniq()
    |> Enum.map(fn inst ->
      inst_id = inst.id
      visited = [inst_id | info["#visited"]]
      # Convert id to a unique id combining parent and child ids
      # put original id in "key", only if the parent has key.  This
      # ensures drill down has unique ids for all nodes, but index and
      # show use the real id.
      if info.key do
        inst
        |> Map.put(:key, inst.id)
        |> Map.put(:id, get_unique_id(info.id, inst))
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
    |> Enum.map(fn item -> atom_to_string(item) end)
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
      |> Enum.map(fn item -> atom_to_string(item) end)
    else
      info.events
      |> Enum.filter(
        &(not (Map.get(&1, :"$partial", false) == true and Map.get(&1, :_confidence, nil) == 0.0))
      )
      |> Enum.sort_by(& &1.id)
      |> Enum.map(fn item -> atom_to_string(item) end)
    end
  end


  defp atom_to_string(data) do
    data
    |> Map.new(fn {k,v} ->
      if is_atom(k) do
      {Atom.to_string(k), v}
      else
        {k, v}
      end
    end)
  end

  defp get_unique_id(info_id, inst) do
    if Map.has_key?(inst, :assertion_id) do
      info_id <> "|" <> inst.id <> "|" <> inst.assertion_id
    else
      info_id <> "|" <> inst.id <> "|" <> ""
    end
  end
end
