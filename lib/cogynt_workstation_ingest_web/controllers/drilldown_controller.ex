defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

  @doc """
  Return a list of the info on all template instances for the given type
  """
  def index(conn, %{"id" => id}) do
    data =
      DrilldownContext.list_template_solutions()
      |> DrilldownContext.process_template_solutions()

    data =
      data
      |> Map.drop([:timer])
      |> Map.values()
      |> Enum.filter(&(&1["template_type_id"] == id))
      |> Enum.map(&Map.put(Map.put(&1, "key", &1["id"]), "#visited", []))

    render(conn, "index.json-api", data: data)
  end

  @doc """
  Respond to a show request for a specific template
  """
  def show(conn, %{"id" => id}) do
    data =
      DrilldownContext.get_template_solution(id)
      |> DrilldownContext.process_template_solution()

    if data == nil do
      render(conn, "404.json")
    else
      data =
        data
        |> Map.put("key", data["id"])
        |> Map.put("#visited", [])

      render(conn, "show.json-api", data: data)
    end
  end
end
