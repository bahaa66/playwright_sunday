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
      %{template_type_id: _}, _ -> :drilldown_solution
      _, _ -> :drilldown_event
    end)
  end

  object :drilldown_edge do
    field :id, non_null(:string)
    field(:from, non_null(:string))
    field(:to, non_null(:string))
  end

  object :drilldown_solution do
    field :id, non_null(:id)
    field :retracted, non_null(:string)
    field :template_type_id, non_null(:id)
    field :template_type_name, non_null(:string)
    field :time, non_null(:string), do: resolve(fn _, _, _ -> {:ok, ""} end)

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

  object :drilldown_event do
    @desc "The core id of the event. This translates to the core id stored for events in workstation."
    field :id, non_null(:id), do: resolve(&DrilldownResolver.get_id/3)
    field :assertion_id, :id
    field :fields, non_null(:json), do: resolve(&DrilldownResolver.get_fields/3)
    field :processed_at, :string
    field :published_at, :string, do: resolve(&DrilldownResolver.get_published_at/3)
    field :published_by, :id, do: resolve(&DrilldownResolver.get_published_by/3)
    field :version, non_null(:integer), do: resolve(&DrilldownResolver.get_version/3)
    field :source, non_null(:string), do: resolve(&DrilldownResolver.get_source/3)
    field :risk_score, :integer, do: resolve(&DrilldownResolver.get_risk_score/3)
    field :data_type, non_null(:string), do: resolve(&DrilldownResolver.get_data_type/3)
  end
end
