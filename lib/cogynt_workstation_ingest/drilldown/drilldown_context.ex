defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  alias Models.Drilldown.TemplateSolutions
  alias CogyntWorkstationIngest.Repo

  # -------------------------------- #
  # --- Drilldown Schema Methods --- #
  # -------------------------------- #

  @doc """
  Lists all the TemplateSolutions stored in the database
  ## Examples
      iex> list_template_solutions()
      [%TemplateSolutions{}, ...]
  """
  def list_template_solutions() do
    case Repo.all(TemplateSolutions) do
      nil ->
        []

      results ->
        process_template_solutions(results)
    end
  end

  @doc """

  """
  def get_template_solution(id), do: Repo.get(TemplateSolutions, id)

  @doc """

  """
  def get_template_solution_data(id) do
    case get_template_solution(id) do
      nil ->
        nil

      data ->
        process_template_solution(data)
    end
  end

  @doc """
  Creates an TemplateSolutions.
  ## Examples
      iex> create_template_solution(%{field: value})
      {:ok, %TemplateSolutions{}}
      iex> create_template_solution(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_template_solution(attrs \\ %{}) do
    %TemplateSolutions{}
    |> TemplateSolutions.changeset(attrs)
    |> Repo.insert()
  end

  @doc """

  """
  def update_template_solution(%TemplateSolutions{} = template_solution, attrs) do
    template_solution
    |> TemplateSolutions.changeset(attrs)
    |> Repo.update()
  end

  @doc """

  """
  def upsert_template_solutions(%{sol_id: id, sol: _sol, evnt: _evnt} = attrs) do
    case get_template_solution(id) do
      nil ->
        build_attrs(nil, attrs)
        |> atomize_map()
        |> create_template_solution()

      template_solution ->
        attrs =
          template_solution
          |> Map.from_struct()
          |> Map.drop([:__meta__])
          |> stringify_map()
          |> build_attrs(attrs)
          |> atomize_map()

        update_template_solution(template_solution, attrs)
    end
  end

  def upsert_template_solutions(%{sol_id: id, sol: sol} = attrs) do
    case get_template_solution(id) do
      nil ->
        %{"events" => %{}, "outcomes" => []}
        |> Map.merge(sol)
        |> atomize_map()
        |> create_template_solution

      template_solution ->
        attrs =
          template_solution
          |> Map.from_struct()
          |> Map.drop([:__meta__])
          |> stringify_map()
          |> build_attrs(attrs)

        update_template_solution(template_solution, attrs)
    end
  end

  @doc """

  """
  def hard_delete_template_solutions_data() do
    try do
      result = Repo.query("TRUNCATE template_solutions", [])

      CogyntLogger.info(
        "#{__MODULE__}",
        "hard_delete_template_solutions_data completed with result: #{result}"
      )
    rescue
      e ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "hard_delete_template_solutions_data failed with reason: #{inspect(e)}"
        )
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp build_attrs(temp_sol, %{sol_id: _id, sol: sol, evnt: evnt} = data) do
    sol = (temp_sol || %{"events" => %{}, "outcomes" => []}) |> Map.merge(sol)

    cond do
      Map.has_key?(data, :event) and not Map.has_key?(data.event, "aid") ->
        sol
        |> Map.put("outcomes", [evnt | sol["outcomes"]])

      Map.has_key?(evnt, "published_by") and sol["id"] == evnt["published_by"] ->
        # event is input and published by same instance
        temp_sol

      Map.has_key?(data, :event) and Map.has_key?(data.event, "aid") ->
        key = evnt["id"] <> "!" <> evnt["assertion_id"]

        sol
        |> Map.put("events", Map.put(sol["events"], key, evnt))

      true ->
        data
    end
  end

  defp build_attrs(_temp_sol, data) do
    data
  end

  defp process_template_solution(data) do
    data
    |> Map.from_struct()
    |> Map.drop([:__meta__, :created_at, :updated_at])
    |> stringify_map()
  end

  defp process_template_solutions(data) when is_list(data) do
    Enum.reduce(data, [], fn d, acc ->
      acc ++ [process_template_solution(d)]
    end)
  end

  defp stringify_map(atom_map) do
    for {key, val} <- atom_map, into: %{}, do: {Atom.to_string(key), val}
  end

  defp atomize_map(string_map) do
    for {key, val} <- string_map, into: %{}, do: {String.to_atom(key), val}
  end
end
