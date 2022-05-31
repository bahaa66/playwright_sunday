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

  def list_template_solutions() do
    sql_query = %{
      query: """
        SELECT DISTINCT id, *
        FROM druid.template_solutions
      """
    }

    Druid.sql_query(sql_query)
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

  def get_template_solution_outcomes(ids) when is_list(ids) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM druid.template_solution_events
        WHERE id=ANY('#{Enum.join(ids, "','")}') and aid IS NULL
        ORDER BY __time DESC
      """
    }

    Druid.sql_query(sql_query)
  end

  def get_template_solution_outcomes(id) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM druid.template_solution_events
        WHERE id=? and aid IS NULL
        ORDER BY __time DESC
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
  end

  def get_template_solution_events(ids) when is_list(ids) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM druid.template_solution_events
        WHERE id=ANY('#{Enum.join(ids, "','")}') AND aid IS NOT NULL
        ORDER BY __time ASC
      """
    }

    Druid.sql_query(sql_query)
  end

  def get_template_solution_events(id) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM template_solution_events
        WHERE id=? AND aid IS NOT NULL
        ORDER BY __time ASC
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
  end
end
