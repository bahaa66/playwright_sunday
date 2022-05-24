defmodule CogyntWorkstationIngestWeb.Resolvers.Drilldown2 do
  import Absinthe.Resolution.Helpers, only: [on_load: 2]
  alias CogyntWorkstationIngest.Config
  alias CogyntGraphql.Utils.Error
  alias CogyntWorkstationIngestWeb.Dataloaders.Druid, as: DruidLoader

  @whitelist [
    Config.published_by_key(),
    Config.published_at_key(),
    Config.source_key(),
    "processed_at",
    "source_type",
    "assertion_id",
    "templateTypeId",
    Config.version_key(),
    "solution_id",
    Config.data_type_key()
  ]

  def drilldown(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    get_solution(solution_id, loader, fn
      {:data_loader_error, %{"errorMessage" => error_message} = error}, _loader ->
        if not is_nil(error.code) and error.code == 400 and
             error_message =~ "not found within 'druid'" do
          {:error,
           Error.new(%{
             message: "Drilldown Datasource not found.",
             code: :not_found,
             details:
               "There was an error when querying for drilldown data for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
             original_error: error,
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}
        else
          {:error,
           Error.new(%{
             message: "An internal server occurred while querying for the drilldown data.",
             code: :internal_server_error,
             details:
               "There was an error when querying for drilldown data for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
             original_error: error,
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}
        end

      nil, _loader ->
        {:error,
         Error.new(%{
           message: "Drilldown solution not found.",
           code: :not_found,
           details:
             "The template solutions datasource did not return a template solution for id: #{solution_id}",
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      template_solution, ts_loader ->
        # Get the outcomes of the solution we are querying for so it will appear on the graph and
        # Include its edges.
        get_outcomes([template_solution["id"]], ts_loader, fn
          outcomes, outcomes_loader ->
            outcomes = Map.get(outcomes, template_solution["id"])

            outcome_edges =
              Enum.map(outcomes, fn event ->
                %{
                  id: "#{template_solution["id"]}:#{Map.get(event, Config.id_key())}",
                  from: template_solution["id"],
                  to: Map.get(event, Config.id_key())
                }
              end)

            get_drilldown([template_solution["id"]], outcomes_loader, fn
              %{solutions: solutions, events: events, edges: edges}, _loader ->
                {:ok,
                 %{
                   id: template_solution["id"],
                   nodes: solutions ++ Map.values(events) ++ outcomes,
                   edges: MapSet.to_list(edges) ++ outcome_edges
                 }}
            end)
        end)
    end)
  end

  # A function that will recursively works backwards from the root solution id to create the
  # drilldown graph.
  def get_drilldown(solution_ids, loader, callback) do
    # Fetch the solution for this level.
    get_solutions(solution_ids, loader, fn
      # If solutions aren't returned then we have reached a leaf of the graph and return empty
      # structures.
      [], solutions_loader ->
        callback.(
          %{solutions: [], events: %{}, edges: MapSet.new()},
          solutions_loader
        )

      solutions, solutions_loader ->
        # Fetch the input events for all the existing solutions.
        Enum.map(solutions, &Map.get(&1, "id"))
        |> get_events(solutions_loader, fn
          events, events_loader ->
            # Get a list of solution ids from output events so we can fetch the next level of
            # solutions.
            new_solution_ids = event_solution_ids(events)

            get_drilldown(new_solution_ids, events_loader, fn
              %{edges: edgs, events: e, solutions: s}, drilldown_loader ->
                # Get our events and edges to pass back to the caller.
                edges = process_edges(events)
                events = process_events(events, e)

                callback.(
                  %{
                    edges: MapSet.union(edges, edgs),
                    events: events,
                    solutions: s ++ solutions
                  },
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
      {:data_loader_error, error}, _loader ->
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
             "The template solutions datasource did not return a template solution for id: #{solution_id}",
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
      {:data_loader_error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while querying for child solutions.",
           code: :internal_server_error,
           details:
             "There was an error when querying for child solutions for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      [], _loader ->
        {:ok, []}

      events, loader ->
        event_solution_ids(events)
        |> get_solutions(loader, fn solutions, _loader -> {:ok, solutions} end)
    end)
  end

  def drilldown_solution_events(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    get_events(solution_id, loader, fn
      {:data_loader_error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution events.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution events for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
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
      {:data_loader_error, original_error}, _loader ->
        {:error,
         Error.new(%{
           message:
             "An internal server occurred while querying for the drilldown solution outcomes.",
           code: :internal_server_error,
           details:
             "There was an error when querying for template solution outcomes for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
           original_error: original_error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}

      outcomes, _loader ->
        {:ok, outcomes || []}
    end)
  end

  def get_id(event, _, _) do
    {:ok, Map.get(event, Config.id_key())}
  end

  def get_fields(event, _, _) do
    {:ok,
     event
     |> Enum.reject(fn {k, _v} ->
       Enum.member?(@whitelist, k)
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

  # A function that gets the produced by ids of a list of events if the event has a produced
  # by id. If it doesn't then it is a user input event.
  defp event_solution_ids(events) do
    Enum.reduce(events, MapSet.new(), fn
      event, a ->
        case Map.get(event, Config.published_by_key()) do
          nil ->
            a

          id ->
            MapSet.put(a, id)
        end
    end)
    |> MapSet.to_list()
  end

  # A function that creates edges for each event in a list of events. One from the event to a
  # solution it is fed into as an input event and if it is also an outcome event (has a produced by id)
  # and edge from the the produced by solution to the event node.
  defp process_edges(events) do
    Enum.reduce(events, MapSet.new(), fn
      event, a ->
        id = Map.get(event, Config.id_key())
        solution_id = Map.get(event, "solution_id")
        published_by = Map.get(event, Config.published_by_key())

        a = MapSet.put(a, %{id: "#{id}:#{solution_id}", from: id, to: solution_id})

        if(published_by,
          do: MapSet.put(a, %{id: "#{published_by}:#{id}", from: published_by, to: id}),
          else: a
        )
    end)
  end

  # A function that ensures we are using the event with the highest version number.
  def process_events(events, existing_events \\ %{}) do
    Enum.reduce(events, existing_events, fn
      e, a ->
        case Map.get(a, Map.get(e, Config.id_key())) do
          nil ->
            Map.put(a, e[Config.id_key()], e)

          cached_event ->
            if(e[Config.version_key()] > cached_event[Config.version_key()],
              do: Map.put(a, e[Config.id_key()], e),
              else: a
            )
        end
    end)
  end
end
