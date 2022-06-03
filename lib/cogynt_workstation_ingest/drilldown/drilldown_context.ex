defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  def get_template_solution_outcomes(ids) when is_list(ids) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM druid.template_solution_events
        WHERE id=ANY('#{Enum.join(ids, "','")}') AND aid IS NULL
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
        WHERE id=? AND aid IS NULL
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
        ORDER BY __time DESC
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
        ORDER BY __time DESC
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Druid.sql_query(sql_query)
  end
end
