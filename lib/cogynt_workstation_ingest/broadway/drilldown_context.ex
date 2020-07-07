defmodule CogyntWorkstationIngest.Broadway.DrilldownContext do
  alias Models.Drilldown.TemplateSolutions
  alias CogyntWorkstationIngest.Repo

  def update_template_solutions(%{
    "id" => id,
    "retracted" => retracted,
    "template_type_id" => template_type_id,
    "template_type_name" => template_type_name
  }= event) do
    case Repo.get(TemplateSolutions, id) do
      nil ->
        %TemplateSolutions{}
        |>TemplateSolutions.changeset(event)
        |> Repo.insert()

      template_solution ->
        {:ok, template_solution}
    end


  end

  def update_template_solution_events(%{sol_id: id, sol: sol, evnt: evnt} = data) do
    IO.inspect(id)
    IO.inspect(sol)
    IO.inspect(evnt)
  end

end
