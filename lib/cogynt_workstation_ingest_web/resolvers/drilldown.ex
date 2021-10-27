defmodule CogyntWorkstationIngestWeb.Resolvers.Drilldown do
  import Absinthe.Resolution.Helpers, only: [on_load: 2]
  alias CogyntWorkstationIngest.Config
  alias CogyntGraphql.Utils.Error
  alias CogyntWorkstationIngestWeb.Dataloaders.Druid, as: DruidLoader

  # ------------------------- #
  # --- module attributes --- #
  # ------------------------- #
  Module.put_attribute(
    __MODULE__,
    :published_by_key,
    Config.published_by_key()
  )

  Module.put_attribute(
    __MODULE__,
    :id_key,
    Config.id_key()
  )

  Module.put_attribute(
    __MODULE__,
    :confidence_key,
    Config.confidence_key()
  )

  Module.put_attribute(
    __MODULE__,
    :whitelist,
    [
      Config.published_by_key(),
      Config.published_at_key(),
      "processed_at",
      "source_type",
      "assertion_id",
      "templateTypeId",
      Config.version_key(),
      # TODO: May be able to remove below at some point
      "solution_id",
      "source",
      "data_type"
    ]
  )

  # ------------------------- #

  def drilldown(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    get_solution(solution_id, loader, fn
      {:error, error}, _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for the drilldown data.",
           code: :internal_server_error,
           details:
             "There was an error when querying for drilldown data for template solution #{
               solution_id
             }. Druid may be down or the datasource may not exist.",
           original_error: error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      nil, _loader ->
        {:error,
         Error.new(%{
           message: "Drilldown solution not found.",
           code: :not_found,
           details:
             "The template solutions datasource did not return a template solution for id: #{
               solution_id
             }",
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      template_solution, ts_loader ->
        get_drilldown([template_solution["id"]], ts_loader, fn
          %{solutions: solutions, events: events, outcomes: outcomes, edges: edges}, _loader ->
            {:ok,
             %{
               id: template_solution["id"],
               nodes: solutions ++ Map.values(events) ++ Map.values(outcomes),
               edges:
                 for {id, edge} <- edges, into: [] do
                   Map.put(edge, :id, id)
                 end
             }}
        end)
    end)
  end

  def get_drilldown(solution_ids, loader, callback) do
    get_solutions(solution_ids, loader, fn
      [], solutions_loader ->
        callback.(
          %{solutions: [], events: %{}, outcomes: %{}, edges: %{}},
          solutions_loader
        )

      solutions, solutions_loader ->
        get_events(solution_ids, solutions_loader, fn
          events, events_loader ->
            get_outcomes(solution_ids, events_loader, fn
              outcomes, outcomes_loader ->
                new_solution_ids = event_solution_ids(events, solution_ids)

                get_drilldown(new_solution_ids, outcomes_loader, fn
                  %{edges: edges, events: e, outcomes: o, solutions: s}, drilldown_loader ->
                    {edges, events} =
                      Enum.reduce(events, {edges, %{}}, fn
                        %{
                          @id_key => id,
                          "solution_id" => solution_id
                        } = event,
                        {edges, events} ->
                          {
                            Map.put(edges, id <> ":" <> solution_id, %{from: id, to: solution_id}),
                            Map.put(events, id, event)
                          }

                        _, a ->
                          a
                      end)

                    {edges, outcomes} =
                      Enum.reduce(outcomes, {edges, %{}}, fn
                        {_, []}, acc ->
                          acc

                        {solution_id, outcomes}, {edges, outcome_acc} ->
                          {
                            Enum.reduce(
                              outcomes,
                              edges,
                              &Map.put(&2, solution_id <> ":" <> &1[@id_key], %{
                                from: solution_id,
                                to: &1[@id_key]
                              })
                            ),
                            Enum.reduce(
                              outcomes,
                              outcome_acc,
                              &Map.put(&2, Map.get(&1, @id_key), &1)
                            )
                          }
                      end)

                    callback.(
                      %{
                        edges: edges,
                        events: Map.merge(e, events),
                        outcomes: Map.merge(o, outcomes),
                        solutions: s ++ solutions
                      },
                      drilldown_loader
                    )
                end)
            end)
        end)
    end)
  end

  def drilldown_solution(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    get_solution(solution_id, loader, fn
      {:error, error}, _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for the drilldown solution.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
           original_error: error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      nil, _loader ->
        {:error,
         Error.new(%{
           message: "Drilldown solution not found.",
           code: :not_found,
           details:
             "The template solutions datasource did not return a template solution for id: #{
               solution_id
             }",
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      template_solution, _loader ->
        {:ok, template_solution}
    end)
  end

  def drilldown_solution_children(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_events(solution_id, loader, fn
      {:error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for child solutions.",
           code: :internal_server_error,
           details:
             "There was an error when querying for child solutions for template solution #{
               solution_id
             }. Druid may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      [], _loader ->
        {:ok, []}

      events, loader ->
        event_solution_ids(events, solution_id)
        |> get_solutions(loader, fn solutions, _loader -> {:ok, solutions} end)
    end)
  end

  def drilldown_solution_events(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_events(solution_id, loader, fn
      {:error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution events.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution events for template solution #{
               solution_id
             }. Druid may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      events, _loader ->
        {:ok, events || []}
    end)
  end

  def drilldown_solution_outcomes(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_outcomes(solution_id, loader, fn
      {:error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution outcomes.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution outcomes for template solution #{
               solution_id
             }. Druid may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      outcomes, _loader ->
        {:ok, outcomes || []}
    end)
  end

  def get_fields(event, _, _) do
    {:ok,
     event
     |> Enum.reject(fn {k, _v} ->
       Enum.member?(@whitelist, k)
     end)
     |> Enum.into(%{})
     |> Map.delete(@id_key)}
  end

  def get_version(event, _, _), do: {:ok, Map.get(event, Config.version_key())}

  def get_risk_score(event, _, _) do
    Map.get(event, Config.confidence_key())
    |> case do
      nil -> {:ok, nil}
      score when is_integer(score) -> {:ok, score * 100}
      score when is_float(score) -> {:ok, trunc(Float.round(score * 100))}
    end
  end

  defp get_solution(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      DruidLoader,
      :template_solutions,
      solution_id
    )
    |> on_load(fn loader ->
      callback.(
        Dataloader.get(
          loader,
          DruidLoader,
          :template_solutions,
          solution_id
        ),
        loader
      )
    end)
  end

  defp get_solutions(solution_ids, loader, callback) when is_list(solution_ids) do
    loader
    |> Dataloader.load_many(
      DruidLoader,
      :template_solutions,
      solution_ids
    )
    |> on_load(fn loader ->
      solutions =
        Dataloader.get_many(
          loader,
          DruidLoader,
          :template_solutions,
          solution_ids
        )
        |> Enum.reject(&is_nil(&1))

      callback.(solutions, loader)
    end)
  end

  defp get_events(solution_ids, loader, callback) when is_list(solution_ids) do
    Dataloader.load_many(
      loader,
      DruidLoader,
      :events,
      solution_ids
    )
    |> on_load(fn loader ->
      events =
        Dataloader.get_many(
          loader,
          DruidLoader,
          :events,
          solution_ids
        )
        |> List.flatten()

      callback.(
        events,
        loader
      )
    end)
  end

  defp get_events(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      DruidLoader,
      :events,
      solution_id
    )
    |> on_load(fn loader ->
      events =
        Dataloader.get(
          loader,
          DruidLoader,
          :events,
          solution_id
        )

      callback.(
        events,
        loader
      )
    end)
  end

  defp get_outcomes(solution_ids, loader, callback) when is_list(solution_ids) do
    loader
    |> Dataloader.load_many(
      DruidLoader,
      :outcomes,
      solution_ids
    )
    |> on_load(fn loader ->
      outcomes =
        Dataloader.get_many(
          loader,
          DruidLoader,
          :outcomes,
          solution_ids
        )

      callback.(
        Enum.zip(solution_ids, outcomes) |> Enum.into(%{}),
        loader
      )
    end)
  end

  defp get_outcomes(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      DruidLoader,
      :outcomes,
      solution_id
    )
    |> on_load(fn loader ->
      outcomes =
        Dataloader.get(
          loader,
          DruidLoader,
          :outcomes,
          solution_id
        )

      callback.(
        outcomes,
        loader
      )
    end)
  end

  defp event_solution_ids(events, exclude_solution_ids) when is_list(exclude_solution_ids) do
    Enum.reduce(events, MapSet.new(), fn
      %{@published_by_key => id}, a ->
        if(id in exclude_solution_ids, do: a, else: MapSet.put(a, id))

      _, a ->
        a
    end)
    |> MapSet.to_list()
  end

  defp event_solution_ids(events, exclude_solution_id) do
    Enum.reduce(events, MapSet.new(), fn
      %{@published_by_key => id}, a ->
        if(id == exclude_solution_id, do: a, else: MapSet.put(a, id))

      _, a ->
        a
    end)
    |> MapSet.to_list()
  end
end
