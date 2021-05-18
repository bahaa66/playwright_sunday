defmodule CogyntWorkstationIngestWeb.DrilldownController do
  use CogyntWorkstationIngestWeb, :controller
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

  @doc """
  Return a list of the info on all template instances for the given type
  """
  # This seems like a really expensive query but I am not sure anything really uses it so
  # we can optimize it later if we must.
  def index(conn, %{"id" => id}) do
    with {:ok, template_solutions} <- DrilldownContext.list_template_solutions(),
         {:ok, data} <- DrilldownContext.process_template_solutions(template_solutions) do
      data =
        data
        |> Enum.filter(&(&1["templateTypeId"] == id))
        |> Enum.map(&Map.put(Map.put(&1, "key", &1["id"]), "#visited", []))

      render(conn, "index.json-api", data: data)
    else
      {:error, %{"errorMessage" => message}} ->
        cond do
          message =~ "Object 'template_solutions' not found within 'druid" ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to query template_solution #{id}. The datasource for the template could " <>
                "not be found in Druid. This could be due to the template solution topic not being " <>
                "in kafka, the topic is empty, or Druid has not had enough time to create the " <>
                "datasource and ingest from the topic."
            )

          message =~ "Object 'template_solution_events' not found within 'druid" ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to query template_solution_events for template_solution #{id}. The datasource " <>
                "for the template could not be found in Druid. This could be due to the template " <>
                "solution events topic not being in kafka, the topic is empty, or Druid has not " <>
                "had enough time to create the datasource and ingest from the topic."
            )
        end

        conn
        |> put_status(:internal_server_error)
        |> render("error.json",
          data: %{message: "An error occured while querying for the template solution."}
        )
    end
  end

  @doc """
  Respond to a show request for a specific template
  """
  def show(conn, %{"id" => id}) do
    with {:ok, template_solution} when not is_nil(template_solution) <-
           DrilldownContext.get_template_solution(id),
         {:ok, data} <- DrilldownContext.process_template_solution(template_solution) do
      data =
        data
        |> Map.put("key", data["id"])
        |> Map.put("#visited", [])

      render(conn, "show.json-api", data: data)
    else
      {:ok, nil} ->
        conn
        |> put_status(:not_found)
        |> render("404.json")

      {:error, %{"errorMessage" => message}} ->
        cond do
          message =~ "Object 'template_solutions' not found within 'druid" ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to query template_solution #{id}. The datasource for the template could " <>
                "not be found in Druid. This could be due to the template solution topic not being " <>
                "in kafka, the topic is empty, or Druid has not had enough time to create the " <>
                "datasource and ingest from the topic."
            )

          message =~ "Object 'template_solution_events' not found within 'druid" ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Unable to query template_solution_events for template_solution #{id}. The datasource " <>
                "for the template could not be found in Druid. This could be due to the template " <>
                "solution events topic not being in kafka, the topic is empty, or Druid has not " <>
                "had enough time to create the datasource and ingest from the topic."
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
