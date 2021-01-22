defmodule CogyntWorkstationIngest.Drilldown.DrilldownContextNew do
  alias Models.Drilldown.TemplateSolution
  alias Models.Drilldown.TemplateSolutionEvents
  alias CogyntWorkstationIngest.Repo
  import Ecto.Query


  # -------------------------------- #
    # --- Drilldown Schema Methods --- #
    # -------------------------------- #

    @doc """
    Lists all the TemplateSolution stored in the database
    ## Examples
        iex> list_template_solutions()
        [%{"id" => 1234}, ...]
    """
    def list_template_solutions() do
      case Repo.all(TemplateSolution) do
        nil ->
          []

        template_solution ->
          process_template_solutions(template_solution)
      end
    end

    @doc """
    Returns all the unique TemplateSolutions
    ## Examples
        iex> get_template_solutions()
        [%TemplateSolutions{}]
        iex> get_template_solutions()
        nil
    """
    def get_template_solutions() do
      from( t in TemplateSolution,
      distinct: t.id)
      |> Repo.all()
     end

    @doc """
    Returns the TemplateSolution for id
    ## Examples
        iex> get_template_solution(id)
        %TemplateSolutions{}
        iex> get_template_solution(invalid_id)
        []
    """
    def get_template_solution(id) do
     from(t in TemplateSolution, where: t.id ==^id,
     limit: 1)
     |> Repo.all()
    end

    @doc """
    Returns the Outcomes for id
    ## Examples
        iex> get_template_solution_outcomes(id)
        [outcomes]
        iex> get_template_solution_outcomes(invalid_id)
        nil
    """
    def get_template_solution_outcomes(id) do
      from(t in TemplateSolutionEvents,
      where: t.id==^id and is_nil(t.aid),
      select: t.event)
      |> Repo.all()
    end

    @doc """
    Returns the Events for id
    ## Examples
        iex> get_template_solution_events(id)
        [events]
        iex> get_template_solution_events(invalid_id)
        nil
    """
    def get_template_solution_events(id) do
      from( t in TemplateSolutionEvents,
      where: t.id==^id and not is_nil(t.aid),
      select: [event: t.event, aid: t.aid, event_id: t.event["id"]])
      |> Repo.all()
    end

    @doc """
    Fetches the TemplateSolution and returns the data as a map with
    string keys
    """
    def get_template_solution_data(id) do
      case get_template_solution(id) do
        [] ->
          nil

        template_solution ->
          template_solution |> List.first() |> process_template_solution()
      end
    end


    # ----------------------- #
    # --- private methods --- #
    # ----------------------- #

    defp process_template_solution(data) do
      template_solution = data
      |> Map.from_struct()
      |> Map.drop([:__meta__])

      outcomes = get_template_solution_outcomes(template_solution.id) |> process_template_solution_outcomes()
      events = get_template_solution_events(template_solution.id) |> process_template_solution_events()

      Map.put(template_solution, :events, events)
      |> Map.put(:outcomes, outcomes)
      |> stringify_map()
    end

    defp process_template_solution_events(events) do
      Enum.reduce(events, %{}, fn evt, acc ->
        evt = Map.new(evt)
        key = evt.event_id <> "!" <> evt.aid
        event = evt.event |> Map.put("assertion_id", evt.aid)
        Map.put(acc, key, event)
      end)
    end

    defp process_template_solution_outcomes(outcomes) do
      Enum.reduce(outcomes, [], fn outcome, acc ->
        outcome = Map.put(outcome, "assertion_id", nil)
        [outcome | acc]
      end)
    end

    defp process_template_solutions(data) when is_list(data) do
      Enum.reduce(data, [], fn d, acc ->
        acc ++ [process_template_solution(d)]
      end)
    end

    defp stringify_map(atom_map) do
      for {key, val} <- atom_map, into: %{}, do: {Atom.to_string(key), val}
    end


  end
