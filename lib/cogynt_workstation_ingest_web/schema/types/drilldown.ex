defmodule CogyntWorkstationIngestWeb.Schema.Types.Drilldown do
  use Absinthe.Schema.Notation
  alias CogyntGraphql.Middleware.Authentication
  alias CogyntWorkstationIngestWeb.Resolvers.Drilldown, as: DrilldownResolver

  object :drilldown_queries do
    field :drilldown_solution, non_null(:drilldown_solution) do
      arg(:id, non_null(:id))

      middleware(Authentication)
      resolve(&DrilldownResolver.drilldown_solution/3)
    end

    field :drilldown, non_null(:drilldown_graph) do
      arg(:id, non_null(:id))

      middleware(Authentication)
      resolve(&DrilldownResolver.drilldown/3)
    end
  end

  object :drilldown_graph do
    field(:edges, non_null(list_of(non_null(:drilldown_edge))))
    field(:nodes, non_null(list_of(non_null(:drilldown_node))))
    field(:id, non_null(:id))
  end

  union :drilldown_node do
    types([:drilldown_solution, :drilldown_event])

    resolve_type(fn
      %{"templateTypeId" => _}, _ -> :drilldown_solution
      _, _ -> :drilldown_event
    end)
  end

  object :drilldown_edge do
    field :id, non_null(:string) do
      resolve(fn %{from: f, to: t}, _, _ -> {:ok, "#{f}:#{t}"} end)
    end

    field(:from, non_null(:string))
    field(:to, non_null(:string))
  end

  object :drilldown_solution do
    field :id, non_null(:id)

    field :attributes, non_null(:drilldown_solution_attributes) do
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
  end

  object :drilldown_solution_attributes do
    field :time, non_null(:string) do
      resolve(fn %{"__time" => time}, _, _ ->
        {:ok, time}
      end)
    end

    field :id, non_null(:id)
    field :retracted, non_null(:string)
    field :template_type_id, non_null(:id)
    field :template_type_name, non_null(:string)
  end

  object :drilldown_event do
    field :id, non_null(:id)

    field :attributes, non_null(:drilldown_event_attributes) do
      resolve(&DrilldownResolver.event_attributes/3)
    end
  end

  object :drilldown_event_attributes do
    field :assertion_id, :id
    field :fields, non_null(:json)
    field :processed_at, :string
    field :published_at, :string
    field :published_by, :id
    field :risk_score, :string
    field :version, :integer
    field :template_type_id, :string
  end
end
