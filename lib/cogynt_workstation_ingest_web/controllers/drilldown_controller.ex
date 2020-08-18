defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller
  # alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache

  @doc """
  Return a list of the info on all template instances for the given type
  """
  def index(conn, %{"id" => id}) do
    case is_authorized?(conn) do
      true ->
        # {:ok, data} = DrilldownContext.list_template_solutions()
        {:ok, data} = DrilldownCache.list()

        if data == nil do
          render(conn, "404.json")
        else
          data =
            data
            |> Map.drop([:timer])
            |> Map.values()
            |> Enum.filter(&(&1.template_type_id == id))
            |> Enum.map(&Map.put(Map.put(&1, :key, &1.id), "#_visited", []))

          render(conn, "index.json-api", data: data)
        end

      false ->
        render(conn, "401.json-api")
    end
  end

  @doc """
  Respond to a show request for a specific template
  """
  def show(conn, %{"id" => id}) do
    case is_authorized?(conn) do
      true ->
        #{:ok, data_pg} = DrilldownContext.get_template_solution_data(id)
        {:ok, data} = DrilldownCache.get(id)
        if data == nil do
          render(conn, "404.json")
        else
          data =
            data
            |> Map.put(:key, data.id)
            |> Map.put("#visited", [])

          render(conn, "show.json-api", data: data)
        end

      false ->
        render(conn, "401.json-api")
    end
  end

  # ---------------------- #
  # -- private methods --- #
  # ---------------------- #
  defp is_authorized?(_conn) do
    true
    # conn = fetch_session(conn)

    # case get_session(conn, :current_user) do
    #   nil ->
    #     false

    #   _user ->
    #     true
    # end
  end

end
