defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  def get_template_solution_outcomes(ids) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid = 'null'
        ORDER BY published_at DESC
      """
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_outcomes(id) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id='#{id}'
        AND aid = 'null'
        ORDER BY published_at DESC
      """
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_events(ids) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid != 'null'
        ORDER BY published_at DESC
      """
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_events(id) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id = '#{id}'
        AND aid != 'null'
        ORDER BY published_at DESC
      """
    }

    Pinot.query(sql_query)
  end
end
