defmodule CogyntWorkstationIngest.Broadway.DrilldownProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DrilldownPipeline.
  """
  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

  @doc """
  process_template_data/1
  """
  def process_template_data(%{event: event} = data) do
    case Map.has_key?(event, :event) do
      false ->
        sol = event

        Map.put(data, :sol_id, sol.id)
        |> Map.put(:sol, sol)

      true ->
        event_new = Map.get(event, :event, nil)

        aid = Map.get(event, :aid, nil)

        evnt =
          if aid != nil do
            event_new
            |> Map.put(:assertion_id, event.aid)
          else
            event_new
          end

        sol = %{
          id: event.id
        }

        Map.put(data, :sol_id, sol.id)
        |> Map.put(:sol, sol)
        |> Map.put(:evnt, evnt)
    end
  end

  @doc """
  update_cache/1 passes the data map object to the DrilldownCache to
  have its state updated with the new data
  """
  def update_cache(data) do
    DrilldownCache.put(data)
  end

  def update_template_solutions(data) do
    DrilldownContext.update_template_solutions(data)
  end
end
