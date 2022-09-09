defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  alias CogyntWorkstationIngest.Pinot.Broker


  def get_template_solution_outcomes(ids) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT id AS solution_id, *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid = 'null'
        ORDER BY publishedAt DESC
      """
    }

    Broker.query(sql_query)
    |> IO.inspect(label: "THE RESPONSE FROM PINOT get_template_solution_outcomes(ids)")
  end

  def get_template_solution_outcomes(id) do
    sql_query = %{
      sql: """
        SELECT id AS solution_id, *
        FROM template_solution_events
        WHERE id='#{id}'
        AND aid = 'null'
        ORDER BY publishedAt DESC
      """
    }

    Broker.query(sql_query)
    |> IO.inspect(label: "THE RESPONSE FROM PINOT get_template_solution_outcomes(id)")
  end

  def get_template_solution_events(ids) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT id AS solution_id, *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid = 'null'
        ORDER BY publishedAt DESC
      """
    }

    Broker.query(sql_query)
    |> IO.inspect(label: "THE RESPONSE FROM PINOT get_template_solution_events(ids)")
  end

  def get_template_solution_events(id) do
    sql_query = %{
      query: """
        SELECT id AS solution_id, *
        FROM template_solution_events
        WHERE id = '#{id}'
        AND aid != 'null'
        ORDER BY publishedAt DESC
      """,
      parameters: [%{type: "VARCHAR", value: id}]
    }

    Broker.query(sql_query)
    |> IO.inspect(label: "THE RESPONSE FROM PINOT get_template_solution_events(id)")
  end

  # def get_template_solution_events(ids) when is_list(ids) do
  #   sql_query = %{
  #     query: """
  #       SELECT id AS solution_id, *
  #       FROM druid.template_solution_events
  #       WHERE id=ANY('#{Enum.join(ids, "','")}') AND aid IS NOT NULL
  #       ORDER BY __time DESC
  #     """
  #   }

  #   Druid.sql_query(sql_query)
  # end

  # def get_template_solution_events(id) do
  #   sql_query = %{
  #     query: """
  #       SELECT id AS solution_id, *
  #       FROM template_solution_events
  #       WHERE id=? AND aid IS NOT NULL
  #       ORDER BY __time DESC
  #     """,
  #     parameters: [%{type: "VARCHAR", value: id}]
  #   }

  #   Druid.sql_query(sql_query)
  # end
end
