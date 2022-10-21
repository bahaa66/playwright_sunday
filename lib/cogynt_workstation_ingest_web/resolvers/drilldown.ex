defmodule CogyntWorkstationIngestWeb.Resolvers.Drilldown do
  import Absinthe.Resolution.Helpers, only: [on_load: 2]
  alias CogyntWorkstationIngest.Config
  alias CogyntGraphql.Utils.Error
  alias CogyntWorkstationIngestWeb.Dataloaders.Pinot, as: PinotLoader

  @whitelist [
    Config.published_by_key(),
    Config.published_at_key(),
    Config.source_key(),
    Config.version_key(),
    Config.data_type_key(),
    Config.crud_key(),
    Config.confidence_key(),
    Config.partial_key(),
    "processed_at",
    "source_type",
    "template_type_id",
    "assertion_id",
    "solution_id"
  ]

  def drilldown(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    if Config.drilldown_enabled?() do
      get_outcomes(solution_id, loader, fn
        [], _ ->
          {:error,
           Error.new(%{
             message: "Outcome event not found.",
             code: :internal_server_error,
             details: "There weren't any outcome events found for solutionid #{solution_id}.",
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}

        result, outcome_loader ->
          build_drilldown([solution_id], outcome_loader, fn
            {:ok, %{nodes: nodes, edges: edges} = drilldown}, _loader ->
              outcomes = result |> Enum.map(&Map.get(&1, "event"))

              edges =
                Enum.reduce(outcomes, edges, fn
                  o, acc ->
                    producer_id = Map.get(o, Config.published_by_key())
                    id = Map.get(o, Config.id_key())

                    if producer_id do
                      # Note: The ids have to be in this order in order for the halo on the front end
                      # to work correctly.
                      MapSet.put(acc, %{id: "#{producer_id}:#{id}", from: producer_id, to: id})
                    else
                      acc
                    end
                end)

              {:ok,
               Map.put(drilldown, :id, solution_id)
               |> Map.put(:nodes, MapSet.union(MapSet.new(outcomes), nodes) |> MapSet.to_list())
               |> Map.put(:edges, edges |> MapSet.to_list())}
          end)
      end)
    else
      {:error,
       Error.new(%{
         message: "Drilldown not enabled.",
         code: :not_found,
         details:
           "Drilldown is not enabled for this environment. You will need to enable the drilldown config and make sure all dependencies are running and reachable.",
         module: "#{__MODULE__} line: #{__ENV__.line}"
       })}
    end
  end

  def drilldown_solution(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    if Config.drilldown_enabled?() do
      get_solution(solution_id, loader, fn
        [data_loader_error: original_error], _loader ->
          {:error,
           Error.new(%{
             message: "An internal server occurred while querying for the drilldown solution.",
             code: :internal_server_error,
             details:
               "There was an error when querying for template solution #{solution_id}. Pinot may be down or the table may not exist.",
             original_error: original_error,
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}

        nil, _loader ->
          {:error,
           Error.new(%{
             message: "Drilldown solution not found.",
             code: :not_found,
             details:
               "The template solutions datasource did not return a template solution for id: #{solution_id}",
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}

        template_solution, _loader ->
          {:ok, template_solution}
      end)
    else
      {:error,
       Error.new(%{
         message: "Drilldown not enabled.",
         code: :not_found,
         details:
           "Drilldown is not enabled for this environment. You will need to enable the drilldown config and make sure all dependencies are running and reachable.",
         module: "#{__MODULE__} line: #{__ENV__.line}"
       })}
    end
  end

  defp get_solution(solution_id, loader, callback) do
    loader
    |> Dataloader.load(
      PinotLoader,
      :template_solutions,
      solution_id
    )
    |> on_load(fn loader ->
      callback.(
        Dataloader.get(
          loader,
          PinotLoader,
          :template_solutions,
          solution_id
        ),
        loader
      )
    end)
  end

  def drilldown_solution_children(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_events(solution_id, loader, fn
      [data_loader_error: original_error], _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for child solutions.",
           code: :internal_server_error,
           details:
             "There was an error when querying for child solutions for template solution #{solution_id}. Pinot may be down or the table may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      [], _loader ->
        {:ok, []}

      events, loader ->
        Enum.map(events, &Map.get(&1, "event"))
        |> event_solution_ids()
        |> get_solutions(loader, fn solutions, _loader -> {:ok, solutions} end)
    end)
  end

  def drilldown_solution_events(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_events(solution_id, loader, fn
      [data_loader_error: original_error], _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution events.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution events for template solution #{solution_id}. Pinot may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      events, _loader ->
        {:ok, Enum.map(events || [], &Map.get(&1, "event"))}
    end)
  end

  def drilldown_solution_outcomes(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_outcomes(solution_id, loader, fn
      [data_loader_error: original_error], _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution outcomes.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution outcomes for template solution #{solution_id}. Pinot may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      outcomes, _loader ->
        outcomes = outcomes |> Enum.map(&Map.get(&1, "event"))
        {:ok, outcomes || []}
    end)
  end

  def get_id(event, _, _) do
    {:ok, Map.get(event, Config.id_key())}
  end

  def get_fields(event, _, _) do
    {:ok,
     event
     |> Enum.reject(fn
       {k, _v} -> Enum.member?(@whitelist, k)
     end)
     |> Enum.into(%{})
     |> Map.delete(Config.id_key())}
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

  def get_source(event, _, _), do: {:ok, Map.get(event, Config.source_key())}

  def get_published_by(event, _, _), do: {:ok, Map.get(event, Config.published_by_key())}

  def get_published_at(event, _, _), do: {:ok, Map.get(event, Config.published_at_key())}

  def get_data_type(event, _, _), do: {:ok, Map.get(event, Config.data_type_key())}

  # Private functions #

  defp build_drilldown(solution_ids, loader, callback) do
    get_events(solution_ids, loader, fn
      [data_loader_error: original_error], _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for child solutions.",
           code: :internal_server_error,
           details:
             "There was an error when querying for child solutions for template solutions #{inspect(solution_ids)}. Pinot may be down or can not be reached.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      [], events_loader ->
        callback.(
          {:ok,
           %{
             nodes: MapSet.new(),
             edges: MapSet.new()
           }},
          events_loader
        )

      events, events_loader ->
        {events, solutions, new_edges, producer_ids} =
          Enum.reduce(events, {MapSet.new(), MapSet.new(), MapSet.new(), MapSet.new()}, fn
            %{
              "solution_id" => s_id,
              "aid" => aid,
              "template_type_id" => type_id,
              "template_type_name" => type_name,
              "event_id" => e_id,
              "event" => event
            },
            {events, solutions, edges, producer_ids} ->
              event = event |> Map.put_new("assertion_id", aid)
              producer_id = Map.get(event, Config.published_by_key())

              producer_ids =
                if(producer_id, do: MapSet.put(producer_ids, producer_id), else: producer_ids)

              solution = %{
                id: s_id,
                template_type_id: type_id,
                template_type_name: type_name,
                retracted: Map.get(event, Config.crud_key()) == "delete"
              }

              edges =
                if(e_id && s_id,
                  # Note: The ids have to be in this order in order for the halo on the front end to work
                  # correctly.
                  do: MapSet.put(edges, %{id: "#{e_id}:#{s_id}", from: e_id, to: s_id}),
                  else: edges
                )

              edges =
                if(e_id && producer_id,
                  do:
                    MapSet.put(edges, %{
                      id: "#{e_id}:#{producer_id}",
                      from: producer_id,
                      to: e_id
                    }),
                  else: edges
                )

              {MapSet.put(events, event), MapSet.put(solutions, solution), edges, producer_ids}
          end)

        build_drilldown(producer_ids |> MapSet.to_list(), events_loader, fn
          {:ok, %{nodes: nodes, edges: edges}}, final_loader ->
            callback.(
              {:ok,
               %{
                 nodes: nodes |> MapSet.union(events) |> MapSet.union(solutions),
                 edges: edges |> MapSet.union(new_edges)
               }},
              final_loader
            )
        end)
    end)
  end

  defp get_solutions(solution_ids, loader, callback) when is_list(solution_ids) do
    loader
    |> Dataloader.load_many(
      PinotLoader,
      :template_solutions,
      solution_ids
    )
    |> on_load(fn loader ->
      solutions =
        Dataloader.get_many(
          loader,
          PinotLoader,
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
      PinotLoader,
      :events,
      solution_ids
    )
    |> on_load(fn loader ->
      events =
        Dataloader.get_many(
          loader,
          PinotLoader,
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
      PinotLoader,
      :events,
      solution_id
    )
    |> on_load(fn loader ->
      events =
        Dataloader.get(
          loader,
          PinotLoader,
          :events,
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
      PinotLoader,
      :outcomes,
      solution_id
    )
    |> on_load(fn loader ->
      outcomes =
        Dataloader.get(
          loader,
          PinotLoader,
          :outcomes,
          solution_id
        )

      callback.(
        outcomes,
        loader
      )
    end)
  end

  # A function that gets the produced by ids of a list of events if the event has a produced
  # by id. If it doesn't then it is a user input event.
  defp event_solution_ids(events) do
    Enum.reduce(events, MapSet.new(), fn
      event, a ->
        published_by = Map.get(event, Config.published_by_key())
        if(published_by, do: MapSet.put(a, published_by), else: a)
    end)
    |> MapSet.to_list()
  end
end
