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
            events = events
            |> latest_events()
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
            events = events
            |> latest_events("{{eventId}}!{{aid}}")
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
        MapSet.to_list(solution_ids)
        |> DrilldownContext.get_template_solution_outcomes()
        |> case do
          {:ok, outcome_events} ->
            outcome_events = latest_events(outcome_events, "{{solution_id}}")

            for id <- MapSet.to_list(solution_ids), into: %{} do
              {id, Map.get(outcome_events, id)}
            end

          {:error, error} ->
            for id <- solution_ids, into: %{} do
              {id, {:data_loader_error, error}}
            end
        end
    end
  end

  defp latest_events(events, parse_key \\ "{{eventId}}") do
    Enum.reduce(events, %{}, fn
      %{
        "version" => version,
        "event" => event
      } = druid_event,
      acc ->
        key = get_key(druid_event, parse_key)
        cached_event = Map.get(acc, key, %{"event" => %{}})
        cached_version = get_in(cached_event, ["event", Config.version_key()]) || 0
        cached_pa = get_in(cached_event, ["event", Config.published_at_key()]) || "1970-01-01T00:00:00Z"

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
                  IO.inspect(%{
                    key: key,
                    cached_version: cached_version,
                    cached_pa: cached_pa,
                    version: version,
                    pa: pa
                  }, label: "VERSION IS GREATER")
                  Map.put(acc, key, druid_event)

                version == cached_version and DateTime.compare(pa, cached_pa) == :gt ->
                  IO.inspect(%{
                    key: key,
                    cached_version: cached_version,
                    cached_pa: cached_pa,
                    version: version,
                    pa: pa
                  }, label: "PUBLISHED AT IS GREATER")
                  Map.put(acc, key, druid_event)

                true ->
                  acc
              end
            else
              {:error, error} -> {:error, :date_parse_error, error}
              :error -> {:error, :version_parse_error}
            end

          {:error, error} ->
            {:error, :json_decode_error, error}
        end
    end)
  end

  defp get_key(event, parse_key) do
    Enum.reduce(event, parse_key, fn
      {k, v}, acc when is_binary(v) ->
        "{{#{k}}}"
        |> Regex.compile!()
        |> Regex.replace(acc, v)

      {k, v}, acc ->
        "{{#{k}}}"
        |> Regex.compile!()
        |> Regex.replace(acc, Jason.encode!(v))
    end)
  end
end
