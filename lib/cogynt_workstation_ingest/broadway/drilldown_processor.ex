defmodule CogyntWorkstationIngest.Broadway.DrilldownProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DrilldownPipeline.
  """
  # alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

  @doc """
  process_template_data/1
  """
  def process_template_data(%{event: event} = data) do
    case event["event"] do
      nil ->
        sol = %{
          "retracted" => event["retracted"],
          "template_type_name" => event["template_type_name"],
          "template_type_id" => event["template_type_id"],
          "id" => event["id"]
        }

        Map.put(data, :sol_id, sol["id"])
        |> Map.put(:sol, sol)

      val ->
        evnt =
          val
          |> Map.put("assertion_id", event["aid"])

        sol = %{
          "id" => event["id"]
        }

        Map.put(data, :sol_id, sol["id"])
        |> Map.put(:sol, sol)
        |> Map.put(:evnt, evnt)
    end
  end

  @doc """
  update_template_solutions/1 passes the data map object to the DrilldownCache to
  have its state updated with the new data
  """
  def update_template_solutions(data) do
    # DrilldownCache.put(data)
    DrilldownContext.update_template_solutions(data)
  end
end
