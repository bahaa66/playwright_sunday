defmodule CogyntWorkstationIngestWeb.Dataloaders.Druid do
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Config

  # ------------------------- #
  # --- module attributes --- #
  # ------------------------- #

  Module.put_attribute(
    __MODULE__,
    :published_at_key,
    Config.published_at_key()
  )

  Module.put_attribute(
    __MODULE__,
    :id_key,
    Config.id_key()
  )

  Module.put_attribute(
    __MODULE__,
    :partial_key,
    Config.partial_key()
  )

  Module.put_attribute(
    __MODULE__,
    :confidence_key,
    Config.confidence_key()
  )

  # ------------------------- #

  @doc """
  Creates a new kv Dataloader source
  """
  def data() do
    Dataloader.KV.new(druid_loader())
  end

  def druid_loader do
    fn
      :outcomes, solution_ids ->
        DrilldownContext.get_template_solution_outcomes(MapSet.to_list(solution_ids))
        |> case do
          {:ok, events} ->
            events =
              events
              |> Enum.reduce(%{}, fn %{"solution_id" => solution_id, "event" => event}, acc ->
                Jason.decode(event)
                |> case do
                  {:ok, event} ->
                    de = Map.get(acc, Map.get(event, @id_key), %{})

                    outcome =
                      Map.put(event, "assertion_id", nil) |> Map.put("solution_id", solution_id)

                    epa = Map.get(de, @published_at_key, "1970-01-01T00:00:00Z")
                    npa = Map.get(outcome, @published_at_key, "1970-01-01T00:00:00Z")

                    if(de == %{} or npa > epa, do: Map.put(acc, outcome["id"], outcome), else: acc)

                  {:error, error} ->
                    {:error, :json_decode_error, error}
                end
              end)
              |> Map.values()
              |> Enum.sort_by(& &1[@id_key])
              |> Enum.group_by(&Map.get(&1, "solution_id"))

            for id <- solution_ids, into: %{} do
              {id, events[id] || []}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:error, error}}
            end
        end

      :events, solution_ids ->
        DrilldownContext.get_template_solution_events(MapSet.to_list(solution_ids))
        |> case do
          {:ok, events} ->
            events =
              events
              |> Enum.map(fn %{"aid" => aid, "solution_id" => solution_id, "event" => event} ->
                Jason.decode(event)
                |> case do
                  {:ok, event} ->
                    event |> Map.put("solution_id", solution_id) |> Map.put("assertion_id", aid)

                  {:error, error} ->
                    {:error, :json_decode_error, error}
                end
              end)
              |> Enum.filter(&(not (&1[@partial_key] == true and &1[@confidence_key] == 0.0)))
              |> Enum.sort_by(& &1[@id_key])
              |> Enum.group_by(&Map.get(&1, "solution_id"))

            for id <- solution_ids, into: %{} do
              {id, events[id] || []}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:error, error}}
            end
        end

      :template_solutions, solution_ids ->
        DrilldownContext.list_template_solutions(%{ids: solution_ids})
        |> case do
          {:ok, solutions} ->
            solutions_map =
              solutions
              |> Enum.into(%{}, fn
                %{"id" => id} = e ->
                  {id, e}
              end)

            for id <- solution_ids, into: %{} do
              {id, solutions_map[id]}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:error, error}}
            end
        end
    end
  end
end