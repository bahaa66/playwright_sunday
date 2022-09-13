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
        DrilldownContext.get_template_solution_outcomes(MapSet.to_list(solution_ids))
        |> case do
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
        DrilldownContext.get_template_solution_events(MapSet.to_list(solution_ids))
        |> case do
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
end
