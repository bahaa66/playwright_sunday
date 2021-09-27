defmodule CogyntWorkstationIngestWeb.Schema do
  use Absinthe.Schema

  import_types(Absinthe.Type.Custom)

  import_types(__MODULE__.Types.{
    Drilldown,
    LivenessCheck
  })

  query do
    import_fields(:drilldown_queries)
    import_fields(:liveness_check_queries)
  end
end
