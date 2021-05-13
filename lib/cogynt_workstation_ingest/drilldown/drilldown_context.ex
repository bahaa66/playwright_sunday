defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  def list_template_solutions(%{ids: ids}) do
    sql_query = %{
      query: """
        SELECT DISTINCT id, *
        FROM druid.template_solutions
        WHERE id=ANY('#{Enum.join(ids, "','")}')
      """
    }

    Druid.sql_query(sql_query)
  end

  def list_template_solutions!(%{ids: ids}) do
    sql_query = %{
      query: """
        SELECT DISTINCT id, *
        FROM druid.template_solutions
        WHERE id=ANY('#{Enum.join(ids, "','")}')
        GROUP BY id
      """
    }

    Druid.sql_query(sql_query)
    |> case do
      {:ok, template_solutions} -> {:ok, template_solutions}
      {:error, error} -> raise "Error querying for template solutions: #{inspect(error)}"
    end
  end

  def list_template_solutions() do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solutions
      """
    }

    Druid.sql_query(sql_query)
  end

  def list_template_solutions!() do
    sql_query = %{
      query: """
        SELECT *
        FROM druid.template_solutions
      """
    }

    Druid.sql_query(sql_query)
    |> case do
      {:ok, template_solutions} -> template_solutions
      {:error, error} -> raise "Error querying for template solutions: #{inspect(error)}"
    end
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
      {:ok, []} -> {:ok, nil}
      {:ok, [template_solution]} -> {:ok, template_solution}
      {:error, error} -> {:error, error}
    end
  end

  def get_template_solution!(id) do
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
      {:error, error} -> raise "Error querying for template solution #{id}: #{inspect(error)}"
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
  end

  def get_template_solution_outcomes!(id) do
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
      {:error, error} -> raise "Error querying for outcomes for #{id}: #{inspect(error)}"
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
  end

  def get_template_solution_events!(id) do
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
      {:error, _error} -> raise "Error querying for events for #{id}: #{inspect(id)}"
    end
  end

  def process_template_solutions(data) when is_list(data) do
    Enum.reduce(data, [], fn d, acc ->
      acc ++ [process_template_solution(d)]
    end)
  end

  def process_template_solution(data) do
    outcomes =
      get_template_solution_outcomes(data["id"])
      |> process_template_solution_outcomes()

    events =
      get_template_solution_events(data["id"])
      |> process_template_solution_events()

    Map.put(data, "events", events)
    |> Map.put("outcomes", outcomes)
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
end
