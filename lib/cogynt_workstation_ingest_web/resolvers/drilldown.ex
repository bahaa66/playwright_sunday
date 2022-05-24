defmodule CogyntWorkstationIngestWeb.Resolvers.Drilldown do
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
    build_drilldown([solution_id], loader, fn
      {:ok, drilldown}, _loader ->
        IO.inspect(drilldown, label: "FINAL DRILLDOWN")
        {:ok, Map.put(drilldown, :id, solution_id)}
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

  # Private functions #

  defp build_drilldown(solution_ids, loader, callback) do
    IO.inspect(solution_ids)

    get_events(solution_ids, loader, fn events, events_loader ->
      Enum.reduce(solution_ids, loader, fn
        id, loader_acc ->
          loader
          |> Dataloader.load(
            DruidLoader,
            :events,
            id
          )
      end)
      |> on_load(fn loader ->
        Enum.reduce(solution_ids, %{nodes: [], edges: []}, fn id, %{nodes: nodes, edges: edges} ->
          events =
            Dataloader.get(
              loader,
              DruidLoader,
              :events,
              id
            )
            |> IO.inspect(label: "EVENTS")

          solutions =
            Enum.map(events, fn
              %{"solution_id" => id} = e ->
                IO.inspect(e)
                %{id: id}
            end)

          %{nodes: nodes ++ events, edges: edges}
        end)

        callback.(
          {:ok,
           %{
             nodes: [],
             edges: []
           }},
          events_loader
        )
      end)
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
end
