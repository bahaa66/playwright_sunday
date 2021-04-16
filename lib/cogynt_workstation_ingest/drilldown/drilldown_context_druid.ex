defmodule CogyntWorkstationIngest.Drilldown.DrilldownContextDruid do
  def list_template_solutions(ids) do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solutions
        WHERE id=ANY(?)
      """,
      parameters: [%{type: "OTHER", value: ids}]
    }

    Druid.sql_query(sql_query) |> IO.inspect()
  end

  def list_template_solutions() do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solutions
      """
    }

    Druid.sql_query(sql_query) |> IO.inspect()
  end

  def get_template_solution(id) do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solutions
        WHERE id=?
        LIMIT 1
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
    |> case do
      {:ok, [template_solution]} -> template_solution
      {:error, _error} -> raise "Could not query druid for template solution #{id}"
    end
  end

  def get_template_solution_outcomes(id) do
    sql_query = %{
      query: """
        SELECT event
        FROM druid.template_solution_events
        WHERE id=? and aid IS NULL
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
    |> case do
      {:ok, template_solutions} -> template_solutions
      {:error, _error} -> raise "Could not query druid for outcomes"
    end
  end

  def get_template_solution_events(id) do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solution_events
        WHERE id=? and aid IS NOT NULL
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
    |> case do
      {:ok, template_solutions} -> template_solutions
      {:error, _error} -> raise "Could not query druid for events"
    end
  end

  def process_template_solution(data) do
    outcomes =
      get_template_solution_outcomes(data["id"])
      |> IO.inspect(label: "OUTCOMES")
      |> process_template_solution_outcomes()

    events =
      get_template_solution_events(data["id"])
      |> process_template_solution_events()

    Map.put(data, :events, events)
    |> Map.put(:outcomes, outcomes)
    |> stringify_map()
  end

  # ------------------------- #
  # --- private functions --- #
  # ------------------------- #

  defp process_template_solution_events(events) do
    Enum.reduce(events, %{}, fn evt, acc ->
      key = evt["event_id"] <> "!" <> evt["aid"]

      evt["event"]
      |> Jason.decode()
      |> case do
        {:ok, event} ->
          event = Map.put(event, "assertion_id", evt["aid"])
          Map.put(acc, key, event)

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Unable to decode template solution event stored in druid #{evt["event"]}, Error: #{
              inspect(error)
            }"
          )

          acc
      end
    end)
  end

  defp process_template_solution_outcomes(outcomes) do
    Enum.reduce(outcomes, %{}, fn %{"event" => event}, acc ->
      event
      |> Jason.decode()
      |> case do
        {:ok, event} ->
          de = Map.get(acc, Map.get(event, "id"), %{})
          outcome = Map.put(event, "assertion_id", nil)
          epa = Map.get(de, "published_at", "1970-01-01T00:00:00Z")
          npa = Map.get(outcome, "published_at", "1970-01-01T00:00:00Z")

          # If the event doesn't already exists in our map or the published_at
          # of this event is more recent than the one in our map we replace it.
          if de == %{} or npa > epa do
            Map.put(acc, Map.get(outcome, "id"), outcome)
          else
            acc
          end

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Unable to decode template solution event outcome stored in druid #{event}, Error: #{
              inspect(error)
            }"
          )

          acc
      end
    end)
    |> Map.values()
  end

  defp stringify_map(atom_map) do
    Enum.reduce(atom_map, %{}, fn
      {key, val}, a when is_atom(key) -> Map.put(a, Atom.to_string(key), val)
      {key, val}, a when is_binary(key) -> Map.put(a, key, val)
    end)
  end
end
