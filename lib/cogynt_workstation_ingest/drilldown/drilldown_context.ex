defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  alias Models.Drilldown.TemplateSolutions
  alias CogyntWorkstationIngest.Repo

  import Ecto.Query

  # -------------------------------- #
  # --- Drilldown Schema Methods --- #
  # -------------------------------- #

  @doc """

  """
  def list_template_solutions() do
    Repo.all(TemplateSolutions)
  end

  @doc """

  """
  def get_template_solution(id), do: Repo.get(TemplateSolutions, id)

  @doc """

  """
  def update_template_solutions(%{sol_id: id, sol: _sol, evnt: _evnt} = data) do
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
        %{events: %{}, outcomes: []}
        |> Map.merge(sol)
        |> create_template_solution

        data

      template_solution ->
        update_record(template_solution, sol)
        data
    end
  end

  defp create_template_solution(attrs) do
    changeset =
      %TemplateSolutions{}
      |> TemplateSolutions.changeset(attrs)

    case Repo.insert(changeset) do
      {:ok, struct} ->
        struct

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "create_template_solution failed with reason: #{inspect(reason)}"
        )
    end
  end

  def get_template_solution_data(id) do
    case get_template_solution(id) do
      nil ->
        {:ok, nil}

      data ->
        {:ok, process_template_solution(data)}
    end
  end

  def hard_delete_template_solutions_data() do
    from(ts in TemplateSolutions)
    |> Repo.delete_all()
  end

  defp get_attrs(temp_sol, %{sol_id: _id, sol: sol, evnt: evnt} = data) do
    sol = (temp_sol || %{events: %{}, outcomes: []}) |> Map.merge(sol)

    cond do
      Map.has_key?(data, :event) and not Map.has_key?(data.event, :aid) ->
        sol
        |> Map.put(:outcomes, [evnt | sol.outcomes])

      Map.has_key?(evnt, :published_by) and sol.id == evnt.published_by ->
        # event is input and published by same instance
        temp_sol

      Map.has_key?(data, :event) and Map.has_key?(data.event, :aid) ->
        key = evnt.id <> "!" <> evnt.assertion_id

        sol
        |> Map.put(:events, Map.put(sol.events, key, evnt))

      true ->
        data
    end
  end

  defp get_attrs(_temp_sol, data) do
    data
  end

  defp update_record(template_solution, data) do
    new_data =
      template_solution
      |> Map.from_struct()
      |> Map.drop([:__meta__])
      |> get_attrs(data)

    temp_sol = TemplateSolutions.changeset(template_solution, new_data)

    case Repo.update(temp_sol) do
      {:ok, _struct} ->
        :ok

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "update_record failed with reason: #{inspect(reason)}"
        )
    end
  end

  defp stringify_map(atom_map) do
    for {key, val} <- atom_map, into: %{}, do: {Atom.to_string(key), val}
  end

  defp process_template_solution(data) do
    data
    |> Map.from_struct()
    |> Map.drop([:__meta__, :created_at, :updated_at])
    |> stringify_map()
  end
end
