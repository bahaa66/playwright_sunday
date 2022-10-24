defmodule CogyntWorkstationIngestWeb.Dataloaders.Pinot do
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Config

  @doc """
  Creates a new kv Dataloader source
  """
  def data() do
    Dataloader.KV.new(pinot_loader())
  end

  def pinot_loader do
    fn
      :outcomes, solution_ids ->
        get_outcomes(MapSet.to_list(solution_ids))
        |> case do
          {:ok, %{exceptions: [%{message: "TableDoesNotExistError"}]}} ->
            for id <- solution_ids, into: %{} do
              {id, []}
            end

          {:ok, events} ->
            events =
              events
              |> decoded_events()
              |> Enum.sort_by(& &1[Config.id_key()])
              |> Enum.group_by(&Map.get(&1, "solution_id"))

            for id <- solution_ids, into: %{} do
              {id, events[id] || []}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:data_loader_error, error}}
            end
        end

      :events, solution_ids ->
        get_events(MapSet.to_list(solution_ids))
        |> case do
          {:ok, %{exceptions: [%{message: "TableDoesNotExistError"}]}} ->
            for id <- solution_ids, into: %{} do
              {id, []}
            end

          {:ok, events} ->
            events =
              events
              |> decoded_events()
              |> Enum.group_by(&Map.get(&1, "solution_id"))

            for id <- solution_ids, into: %{} do
              {id, events[id] || []}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:data_loader_error, error}}
            end
        end

      :template_solutions, solution_ids ->
        MapSet.to_list(solution_ids)
        |> DrilldownContext.get_template_solution_outcomes()
        |> case do
          {:ok, %{exceptions: [%{message: "TableDoesNotExistError"}]}} ->
            for id <- solution_ids, into: %{} do
              {id, []}
            end

          {:ok, outcome_events} ->
            for id <- MapSet.to_list(solution_ids), into: %{} do
              {id, Map.get(decoded_events(outcome_events), id)}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:data_loader_error, error}}
            end
        end
    end
  end

  defp decoded_events(events) do
    Enum.map(events, fn %{"event" => event} = pinot_event ->
      Jason.decode(event)
      |> case do
        {:ok, event} ->
          Map.put(pinot_event, "event", event)

        {:error, error} ->
          {:error, :json_decode_error, error}
      end
    end)
  end

  defp get_events(solution_ids, offset \\ 0) do
    DrilldownContext.get_template_solution_events(solution_ids, offset: offset)
    |> case do
      {:ok, events} when is_list(events) and length(events) > 0 ->
        case get_events(solution_ids, offset + length(events)) do
          {:ok, next} -> {:ok, next ++ events}
          {:error, error} -> {:error, error}
        end

      {:ok, []} ->
        {:ok, []}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Error querying for template solition events starting at offset #{offset}. Error: #{inspect(error)}"
        )
        {:error, error}
    end
  end

  defp get_outcomes(solution_ids, offset \\ 0) do
    DrilldownContext.get_template_solution_outcomes(solution_ids, offset: offset)
    |> case do
      {:ok, outcomes} when is_list(outcomes) and length(outcomes) > 0 ->
        case get_outcomes(solution_ids, offset + length(outcomes)) do
          {:ok, next} -> {:ok, next ++ outcomes}
          {:error, error} -> {:error, error}
        end

      {:ok, []} ->
        {:ok, []}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Error querying for template solition outcomes starting at offset #{offset}. Error: #{inspect(error)}"
        )
        {:error, error}
    end
  end
end
