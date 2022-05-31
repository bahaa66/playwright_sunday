defmodule CogyntWorkstationIngestWeb.Dataloaders.Druid do
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias CogyntWorkstationIngest.Config

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
              |> Enum.reduce(%{}, fn %{"solution_id" => solution_id, "version" => version, "eventId" => e_id, "event" => event}, acc ->
                Jason.decode(event)
                |> case do
                  {:ok, event} ->
                    outcome =
                      Map.put(event, "assertion_id", nil) |> Map.put("solution_id", solution_id)
                    pa = Map.get(outcome, Config.published_at_key(), "1970-01-01T00:00:00Z")
                    cached_outcome = Map.get(acc, e_id, %{})
                    cached_pa = Map.get(cached_outcome, Config.published_at_key(), "1970-01-01T00:00:00Z")
                    cached_version = Map.get(cached_outcome, Config.version_key(), 0)

                    with {version, _} <- Integer.parse(version),
                      {:ok, pa, _} <- DateTime.from_iso8601(pa),
                      {:ok, cached_pa, _} <- DateTime.from_iso8601(cached_pa) do
                      cond do
                        version > cached_version ->
                          Map.put(acc, e_id, outcome)

                        version == cached_version and DateTime.compare(pa, cached_pa) == :gt ->
                          Map.put(acc, e_id, outcome)

                        true ->
                          acc
                      end
                    else
                      {:error, _} -> acc
                      :error -> acc
                    end

                  {:error, error} ->
                    {:error, :json_decode_error, error}
                end
              end)
              |> Map.values()
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
              |> Enum.reduce(%{}, fn
                %{
                  "eventId" => event_id,
                  "aid" => aid,
                  "version" => version,
                  "event" => event
                } = druid_event,
                acc ->
                  key = "#{event_id}!#{aid}"
                  cached_event = Map.get(acc, key, %{})
                  cached_version = Map.get(cached_event, Config.version_key(), 0)
                  cached_pa = Map.get(cached_event, Config.published_at_key(), "1970-01-01T00:00:00Z")

                  Jason.decode(event)
                  |> case do
                    {:ok, event} ->
                      druid_event = Map.put(druid_event, "event", event)
                      pa = Map.get(event, Config.published_at_key(), "1970-01-01T00:00:00Z")

                      with {version, _} <- Integer.parse(version),
                        {:ok, pa, _} <- DateTime.from_iso8601(pa),
                        {:ok, cached_pa, _} <- DateTime.from_iso8601(cached_pa) do
                        cond do
                          version > cached_version ->
                            Map.put(acc, key, druid_event)

                          version == cached_version and DateTime.compare(pa, cached_pa) == :gt ->
                            Map.put(acc, key, druid_event)

                          true ->
                            acc
                        end
                      else
                        {:error, _} -> acc
                        :error -> acc
                      end

                    {:error, error} ->
                      {:error, :json_decode_error, error}
                  end
              end)
              |> Map.values()
              |> List.flatten()
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
              {id, {:data_loader_error, error}}
            end
        end
    end
  end
end
