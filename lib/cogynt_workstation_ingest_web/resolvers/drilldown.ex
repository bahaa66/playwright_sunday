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
      events, events_loader ->
        solution_ids = event_solution_ids(events, exclude_solution_ids)
        published_by_to_solution = Enum.reduce(events, %{}, fn
          %{"published_by" => p, "solution_id" => s_id}, acc ->
            Map.put(acc, p, s_id)

          _, acc ->
            acc
        end)
        get_solutions(solution_ids, events_loader, fn
          [], template_solutions_loader ->
            callback.(
              %{nodes: template_solutions, edges: []},
              template_solutions_loader
            )

          solutions, template_solutions_loader->
            get_drilldown(solutions, template_solutions_loader, fn
              %{nodes: nodes, edges: edges}, drilldown_loader->
                edges_new = Enum.map(solutions, fn
                  %{"id" => id} ->
                    %{
                      from: Map.get(published_by_to_solution, id),
                      to: id
                    }
                end)
                callback.(
                  %{nodes: nodes ++ template_solutions, edges: edges ++ edges_new},
                  drilldown_loader
                )
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

      events, _loader ->
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

  def drilldown_solution_outcomes(%{"id" => solution_id} = parent, _, %{
        context: %{loader: loader}
      }) do
    IO.inspect(parent)
    IO.inspect(loader)
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
    solution_ids
    |> Enum.reduce(loader, fn i, a ->
      a
      |> Dataloader.load(
        DruidLoader,
        :template_solutions,
        i
      )
    end)
    |> on_load(fn loader ->
      solutions =
        Enum.map(solution_ids, fn id ->
          Dataloader.get(
            loader,
            DruidLoader,
            :template_solutions,
            id
          )
        end)

      callback.(solutions, loader)
    end)
  end

  defp get_events(solution_ids, loader, callback) when is_list(solution_ids) do
    Enum.reduce(solution_ids, loader, fn
      id, l ->
        Dataloader.load(
          l,
          DruidLoader,
          {:template_solution_events, :events},
          id
        )
    end)
    |> on_load(fn loader ->
      callback.(
        Enum.reduce(solution_ids, [], fn
          {:error, _}, events ->
            events

          id, acc_events ->
            acc_events ++ Dataloader.get(
              loader,
              DruidLoader,
              {:template_solution_events, :events},
              id
            )
        end),
        loader
      )
    end)
  end

  defp get_events(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      DruidLoader,
      {:template_solution_events, :events},
      solution_id
    )
    |> on_load(fn loader ->
      events = Dataloader.get(
          loader,
          DruidLoader,
          {:template_solution_events, :events},
          solution_id
        )
      callback.(
        events,
        loader
      )
    end)
  end

  defp get_outcomes(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      DruidLoader,
      {:template_solution_events, :outcomes},
      solution_id
    )
    |> on_load(fn loader ->
      IO.inspect(loader)
      outcomes = Dataloader.get(
        loader,
        DruidLoader,
        {:template_solution_events, :outcomes},
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
