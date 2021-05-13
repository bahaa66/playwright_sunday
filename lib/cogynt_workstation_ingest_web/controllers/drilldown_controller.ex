defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

  @doc """
  Return a list of the info on all template instances for the given type
  """
  def index(conn, %{"id" => id}) do
    case DrilldownContext.list_template_solutions() do
      {:ok, template_solutions} ->
        data =
          DrilldownContext.process_template_solutions(template_solutions)
          |> Map.drop([:timer])
          |> Map.values()
          |> Enum.filter(&(&1["template_type_id"] == id))
          |> Enum.map(&Map.put(Map.put(&1, "key", &1["id"]), "#visited", []))

        render(conn, "index.json-api", data: data)
    end
  end

  @doc """
  Respond to a show request for a specific template
  """
  def show(conn, %{"id" => id}) do
    case DrilldownContext.get_template_solution(id) do
      {:ok, nil} ->
        conn
        |> put_status(:not_found)
        |> render("404.json")

      {:ok, template_solution} ->
        IO.inspect(template_solution)
        data = DrilldownContext.process_template_solution(template_solution)

        data =
          data
          |> Map.put("key", data["id"])
          |> Map.put("#visited", [])
          |> IO.inspect()

        render(conn, "show.json-api", data: data)

      {:error, %{"errorMessage" => message}} ->
        if message =~ "Object 'template_solutions' not found within 'druid" do
          CogyntLogger.error(
            "#{__MODULE__}",
            "Unable to query template_solution #{id}. The datasource for the template could " <>
              "not be found in druid. This could be due to the template solution topic not being " <>
              "in kafka, the topic is empty, or Druid has not had enough time to create the " <>
              "datasource and ingest from the topic."
          )
        end

        conn
        |> put_status(:internal_server_error)
        |> render("error.json",
          data: %{message: "An error occured while querying for the template solution."}
        )
    end
  end
end
