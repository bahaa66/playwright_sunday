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

  def drilldown_solution(_, %{id: solution_id}, %{
        context: %{loader: loader}
      }) do
    loader
    |> Dataloader.load(
      DruidLoader,
      :template_solutions,
      solution_id
    )
    |> on_load(fn loader ->
      Dataloader.get(
        loader,
        DruidLoader,
        :template_solutions,
        solution_id
      )
      |> case do
        {:error, error} ->
          {:error,
           Error.new(%{
             message: "An internal server occurred while querying for the drilldown solution.",
             code: :internal_server_error,
             details:
               "There was an error when querying for template solution #{solution_id}. Druid may be down or the datasource may not exist.",
             original_error: error,
             module: "#{__MODULE__} line: #{__ENV__.line}"
           })}

        nil ->
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

        template_solution ->
          {:ok, template_solution}
      end
    end)
  end

  def drilldown_solution_children(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    loader
    |> Dataloader.load(
      DruidLoader,
      {:template_solution_events, :events},
      solution_id
    )
    |> on_load(fn loader ->
      Dataloader.get(
        loader,
        DruidLoader,
        {:template_solution_events, :events},
        solution_id
      )
      |> case do
        {:error, original_error} ->
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

        nil ->
          {:ok, []}

        events ->
          solution_ids =
            Enum.reduce(events, MapSet.new(), fn
              %{"published_by" => id}, a ->
                if(id == solution_id, do: a, else: MapSet.put(a, id))

              _, a ->
                a
            end)
            |> MapSet.to_list()

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

            {:ok, solutions}
          end)
      end
    end)
  end

  def drilldown_solution_events(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    loader
    |> Dataloader.load(
      DruidLoader,
      {:template_solution_events, :events},
      solution_id
    )
    |> on_load(fn loader ->
      Dataloader.get(
        loader,
        DruidLoader,
        {:template_solution_events, :events},
        solution_id
      )
      |> case do
        {:error, original_error} ->
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

        events ->
          {:ok, events || []}
      end
    end)
  end

  def drilldown_solution_outcomes(%{"id" => solution_id}, _, %{
        context: %{loader: loader}
      }) do
    loader
    |> Dataloader.load(
      DruidLoader,
      {:template_solution_events, :outcomes},
      solution_id
    )
    |> on_load(fn loader ->
      Dataloader.get(
        loader,
        DruidLoader,
        {:template_solution_events, :outcomes},
        solution_id
      )
      |> case do
        {:error, original_error} ->
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

        outcomes ->
          {:ok, outcomes || []}
      end
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
end
