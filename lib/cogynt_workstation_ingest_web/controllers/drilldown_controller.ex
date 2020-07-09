defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller

  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache
  alias CogyntWorkstationIngest.Repo

  @doc """
  Return a list of the info on all template instances for the given type
  """
  def index(conn, %{"id" => id}) do
    case is_authorized?(conn) do
      true ->
        {:ok, data} = DrilldownCache.list()

        data =
          data
          |> Map.drop([:timer])
          |> Map.values()
          |> Enum.filter(&(&1["template_type_id"] == id))
          |> Enum.map(&Map.put(Map.put(&1, "key", &1["id"]), "#visited", []))

        render(conn, "index.json-api", data: data)

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
         {:ok, data} = DrilldownCache.get(id)

        new_data = Repo.get(Models.Drilldown.TemplateSolutions, id)
        |> Map.from_struct()
        |> Map.drop([:__meta__, :created_at, :updated_at])
        |> stringify_map()

        # IO.puts("*************IS EQUAL**********")
        # IO.inspect Kernel.map_size(data)
        # IO.inspect Kernel.map_size(new_data)
        # IO.inspect Map.keys(data)
        # IO.inspect Map.keys(new_data)
        # IO.inspect Map.get(data, "template_type_name")
        # IO.inspect Map.get(new_data, "template_type_name")
        # IO.inspect Map.equal?(Map.get(data, "events"), Map.get(new_data, "events"))
        # IO.puts("***********************")

        data = new_data
        if data == nil do
          render(conn, "404.json")
        else
          data =
            data
            |> Map.put("key", data["id"])
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

  defp stringify_map(atom_map) do
    for {key, val} <- atom_map, into: %{}, do: {Atom.to_string(key), val}
  end
end
