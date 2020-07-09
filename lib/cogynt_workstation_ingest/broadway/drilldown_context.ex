defmodule CogyntWorkstationIngest.Broadway.DrilldownContext do
  alias Models.Drilldown.TemplateSolutions
  alias CogyntWorkstationIngest.Repo

  def update_template_solutions( %{sol_id: id, sol: sol, evnt: evnt} = data) do

    case get_template_solution(id) do
      nil ->
        get_attrs(nil, data) |> create_template_solution()
        data

      template_solution ->
        update_record(template_solution, data)
        data
    end


  end

  def update_template_solutions(%{sol_id: id, sol: sol} = data) do

    case get_template_solution(id) do
      nil ->
        %{"events" => %{}, "outcomes" => []}
        |> Map.merge(sol)
        |> create_template_solution

        data

        template_solution ->
        # template_solution |> Map.from_struct() |> Map.drop([:__meta__])
        update_record(template_solution, data.sol)
        data
    end
  end

  def get_template_solution(id), do: Repo.get(TemplateSolutions, id)

  defp create_template_solution(attrs) do
    %TemplateSolutions{}
    |> TemplateSolutions.changeset(attrs)
    |> Repo.insert()

  end

  defp get_attrs(temp_sol, %{sol_id: id, sol: sol, evnt: evnt} = data) do
    sol =  (temp_sol || %{"events" => %{}, "outcomes" => []})
    |> Map.merge(sol)


      result = cond do
        Map.has_key?(data, :event) and not Map.has_key?(data.event, "aid") ->
          sol =
           sol
            |> Map.put("outcomes", [evnt | sol["outcomes"]])


        sol["id"] == evnt["published_by"] ->
          # event is input and published by same instance
          temp_sol

        Map.has_key?(data, :event) and Map.has_key?(data.event, "aid") ->
          key = evnt["id"] <> "!" <> evnt["assertion_id"]
          replace = sol["events"][key]

          if replace != nil do
            # IO.inspect(evnt, label: "@@@@ Received event")
            # IO.inspect(replace, label: "@@@@ Replacing")
          end

          sol =
            sol
            |> Map.put("events", Map.put(sol["events"], key, evnt))

        true ->
          data
      end

  end

  defp get_attrs(_temp_sol, data) do
    data
  end

  defp update_record(template_solution, data) do
    new_data = template_solution
    |> Map.from_struct()
    |> Map.drop([:__meta__])
    |> stringify_map()
    |> get_attrs(data)

    temp_sol = TemplateSolutions.changeset(template_solution, new_data)
    case Repo.update(temp_sol) do
      {:ok, struct} -> :ok
      {:error, changeset} -> :ok
    end
  end

  defp stringify_map(atom_map) do
    for {key, val} <- atom_map, into: %{}, do: {Atom.to_string(key), val}
  end

  defp atomize_map(string_map) do
    for {key, val} <- string_map, into: %{}, do: {String.to_atom(key), val}
  end

end
