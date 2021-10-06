defmodule CogyntWorkstationIngestWeb.Schema.Types.Drilldown do
  use Absinthe.Schema.Notation
  alias CogyntWorkstationIngestWeb.Resolvers.Drilldown, as: DrilldownResolver

  object :drilldown_queries do
    field :drilldown_solution, non_null(:drilldown_solution) do
      arg(:id, non_null(:id))

      resolve(&DrilldownResolver.drilldown_solution/3)
    end

    field :drilldown, non_null(:drilldown_graph) do
      arg(:id, non_null(:id))

      resolve(&DrilldownResolver.drilldown/3)
    end
  end

  interface :drilldown_entity do
    field :id, non_null(:id)
    field :attributes, non_null(:json)

    resolve_type(fn
      %{"templateTypeId" => _} = i, _ ->
        :drilldown_solution

      i, _ ->
        :drilldown_event
    end)
  end

  object :drilldown_solution do
    field :id, non_null(:id)

    field :attributes, non_null(:json) do
      resolve(&DrilldownResolver.solution_attributes/3)
    end

    field :events, non_null(list_of(non_null(:drilldown_event))) do
      resolve(&DrilldownResolver.drilldown_solution_events/3)
    end

    field :outcomes, non_null(list_of(non_null(:drilldown_event))) do
      resolve(&DrilldownResolver.drilldown_solution_outcomes/3)
    end

    field :children, non_null(list_of(non_null(:drilldown_solution))) do
      resolve(&DrilldownResolver.drilldown_solution_children/3)
    end

    interface(:drilldown_entity)
  end

  object :drilldown_event do
    field :id, non_null(:id)

    field :attributes, non_null(:json) do
      resolve(&DrilldownResolver.event_attributes/3)
    end

    interface(:drilldown_entity)
  end

  object :drilldown_graph do
    field(:edges, non_null(list_of(non_null(:drilldown_edge))))
    field(:nodes, non_null(list_of(non_null(:drilldown_entity))))
    field(:id, non_null(:id))
  end

  object :drilldown_edge do
    field :id, non_null(:string) do
      resolve(fn %{from: f, to: t}, _, _ -> {:ok, "#{f}:#{t}"} end)
    end

    field(:from, non_null(:string))
    field(:to, non_null(:string))
  end
end
