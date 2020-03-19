defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller

  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache

  @doc """
  Return a list of the info on all template instances for the given type
  """
  def index(conn, %{"id" => id}) do
    {:ok, data} = DrilldownCache.list()

    data =
      data
      |> Map.values()
      |> Enum.filter(&(&1["template_type_id"] == id))
      |> Enum.map(&Map.put(Map.put(&1, "key", &1["id"]), "#visited", []))

    render(conn, "index.json-api", data: data)
  end

  @doc """
  Respond to a show request for a specific template
  """
  def show(conn, %{"id" => id}) do
    {:ok, data} = DrilldownCache.get(id)

    if data == nil do
      render(conn, "404.json-api")
    else
      data =
        data
        |> Map.put("key", data["id"])
        |> Map.put("#visited", [])

      render(conn, "show.json-api", data: data)
    end
  end
end
