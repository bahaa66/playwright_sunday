defmodule CogyntWorkstationIngestWeb.Resolvers.Drilldown do
  import Absinthe.Resolution.Helpers, only: [on_load: 2]
  alias Absinthe.Utils, as: AbsintheUtils
  alias CogyntGraphql.Utils.Error
  alias CogyntWorkstationIngestWeb.Dataloaders.Druid, as: DruidLoader

  @whitelist [
    "source",
    "published_by",
    "publishing_template_type",
    "publishing_template_type_name",
    "published_at",
    "processed_at",
    "source_type",
    "data_type",
    "assertion_id"
  ]

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
        get_drilldown([template_solution], ts_loader, fn
          accumulator, _loader ->
            {:ok, Map.put(accumulator, :id, template_solution["id"])}
        end)
    end)
  end

  def get_drilldown(template_solutions, loader, callback) do
    exclude_solution_ids = Enum.map(template_solutions, &Map.get(&1, "id"))

    get_events(exclude_solution_ids, loader, fn
      events, events_and_outcomes_loader ->
        solution_ids = event_solution_ids(events, exclude_solution_ids)

        event_edges =
          Enum.reduce(events, [], fn
            %{"id" => id, "solution_id" => s_id}, edges ->
              [%{from: id, to: s_id} | edges]

            _, acc ->
              acc
          end)

        get_solutions(solution_ids, events_and_outcomes_loader, fn
          [], template_solutions_loader ->
            callback.(
              %{nodes: template_solutions ++ events, edges: event_edges},
              template_solutions_loader
            )

          solutions, template_solutions_loader ->
            get_outcomes(Enum.map(solutions, &Map.get(&1, "id")), template_solutions_loader, fn
              outcomes, outcomes_loader ->
                outcome_edges =
                  Enum.map(outcomes, fn {s_id, outcomes} ->
                    Enum.map(outcomes, fn %{"id" => o_id} ->
                      %{
                        from: s_id,
                        to: o_id
                      }
                    end)
                  end)
                  |> List.flatten()

                get_drilldown(solutions, outcomes_loader, fn
                  %{nodes: nodes, edges: edges}, drilldown_loader ->
                    callback.(
                      %{
                        nodes: nodes ++ template_solutions ++ events,
                        edges: edges ++ event_edges ++ outcome_edges
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

  def solution_attributes(template_solution, _, _) do
    {:ok, Map.drop(template_solution, ["id0"])}
  end

  def event_attributes(%{"event" => {:error, :json_decode_error, original_error}}, _, _) do
    {:error,
     Error.new(%{
       message:
         "An internal server occurred while processing the template solution event fields.",
       code: :internal_server_error,
       details: "There was an error json decoding the event fields.",
       original_error: original_error,
       module: "#{__MODULE__} line: #{__ENV__.line}"
     })}
  end

  def event_attributes(event, _, _) do
    fields =
      event
      |> Enum.reject(fn {k, _v} ->
        Enum.member?(@whitelist, k)
      end)
      |> Enum.into(%{})
      |> Map.delete("id")

    attrs =
      event
      |> Enum.filter(fn {k, _v} ->
        Enum.member?(@whitelist, k)
      end)
      |> Enum.into(%{}, fn {k, v} ->
        {AbsintheUtils.camelize(k, lower: true), v}
      end)
      |> Map.put("fields", fields)
      |> IO.inspect()

    {:ok, attrs}
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
      %{"published_by" => id}, a ->
        if(id in exclude_solution_ids, do: a, else: MapSet.put(a, id))

      _, a ->
        a
    end)
    |> MapSet.to_list()
  end

  defp event_solution_ids(events, exclude_solution_id) do
    Enum.reduce(events, MapSet.new(), fn
      %{"published_by" => id}, a ->
        if(id == exclude_solution_id, do: a, else: MapSet.put(a, id))

      _, a ->
        a
    end)
    |> MapSet.to_list()
  end
end
