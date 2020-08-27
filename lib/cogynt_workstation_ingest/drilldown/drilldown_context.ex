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
      [%{"id" => 1234}, ...]
  """
  def list_template_solutions() do
    case Repo.all(TemplateSolutions) do
      nil ->
        []

      template_solutions ->
        process_template_solutions(template_solutions)
    end
  end

  @doc """
  Returns the TemplateSolution for id
  ## Examples
      iex> get_template_solution(id)
      %TemplateSolutions{}
      iex> get_template_solution(invalid_id)
      nil
  """
  def get_template_solution(id), do: Repo.get(TemplateSolutions, id)

  @doc """
  Fetches the TemplateSolution and returns the data as a map with
  string keys
  """
  def get_template_solution_data(id) do
    case get_template_solution(id) do
      nil ->
        nil

      template_solution ->
        process_template_solution(template_solution)
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
  Updates an TemplateSolutions.
  ## Examples
      iex> update_template_solution(template_solution, %{field: new_value})
      {:ok, %TemplateSolutions{}}
      iex> update_template_solution(template_solution, %{field: bad_value})
      {:error, ...}
  """
  def update_template_solution(%TemplateSolutions{} = template_solution, attrs) do
    template_solution
    |> TemplateSolutions.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Will create the TemplateSolution if no record is found for the solution_id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_template_solutions(%{field: value})
      {:ok, %TemplateSolutions{}}
      iex> upsert_template_solutions(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def upsert_template_solutions(
        %{solution_id: solution_id, solution: _solution, solution_event: _solution_event} = attrs
      ) do
    case get_template_solution(solution_id) do
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

  def upsert_template_solutions(%{solution_id: solution_id, solution: solution} = attrs) do
    case get_template_solution(solution_id) do
      nil ->
        %{"events" => %{}, "outcomes" => []}
        |> Map.merge(solution)
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
  Truncates the template_solutions table.
    ## Examples
      iex> hard_delete_template_solutions_data()
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
  defp build_attrs(
         template_solution,
         %{solution_id: _solution_id, solution: solution, solution_event: solution_event} = data
       ) do
    solution = (template_solution || %{"events" => %{}, "outcomes" => []}) |> Map.merge(solution)

    cond do
      Map.has_key?(data, :event) and not Map.has_key?(data.event, "aid") ->
        solution
        |> Map.put("outcomes", [solution_event | solution["outcomes"]])

      Map.has_key?(solution_event, "published_by") and
          solution["id"] == solution_event["published_by"] ->
        # event is input and published by same instance
        template_solution

      Map.has_key?(data, :event) and Map.has_key?(data.event, "aid") ->
        key = solution_event["id"] <> "!" <> solution_event["assertion_id"]

        solution
        |> Map.put("events", Map.put(solution["events"], key, solution_event))

      true ->
        data
    end
  end

  defp build_attrs(_template_solution, data) do
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
