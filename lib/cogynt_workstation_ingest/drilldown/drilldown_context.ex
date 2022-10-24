defmodule CogyntWorkstationIngest.Drilldown.DrilldownContext do
  def get_template_solution_outcomes(ids, opts \\ [limit: 10000])
  def get_template_solution_outcomes(ids, opts) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid = 'null'
        ORDER BY published_at DESC
      """
      |> apply_limit(opts)
      |> apply_offset(opts)
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_outcomes(id, opts) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id='#{id}'
        AND aid = 'null'
        ORDER BY published_at DESC
      """
      |> apply_limit(opts)
      |> apply_offset(opts)
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_events(ids, opts \\ [limit: 10000])
  def get_template_solution_events(ids, opts) when is_list(ids) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id IN ('#{Enum.join(ids, "','")}')
        AND aid != 'null'
        ORDER BY published_at DESC
      """
      |> apply_limit(opts)
      |> apply_offset(opts)
    }

    Pinot.query(sql_query)
  end

  def get_template_solution_events(id, opts) do
    sql_query = %{
      sql: """
        SELECT *
        FROM template_solution_events
        WHERE id = '#{id}'
        AND aid != 'null'
        ORDER BY published_at DESC
      """
      |> apply_limit(opts)
      |> apply_offset(opts)
    }

    Pinot.query(sql_query)
  end

  def apply_limit(query, opts) do
    case Keyword.get(opts, :limit) do
      nil -> query
      limit when is_integer(limit) -> query <> "\nLIMIT #{limit}"
      _bad_arg -> raise ":limit option must be an integer"
    end
  end

  def apply_offset(query, opts) do
    case Keyword.get(opts, :offset) do
      nil -> query
      offset when is_integer(offset) -> query <> "\nOFFSET #{offset}"
      _bad_arg -> raise ":offset option must be an integer"
    end
  end
end
