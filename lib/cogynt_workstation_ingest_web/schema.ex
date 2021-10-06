defmodule CogyntWorkstationIngestWeb.Schema do
  use Absinthe.Schema
  alias CogyntGraphql.Middleware.{ErrorHandler, ErrorException}
  alias CogyntWorkstationIngestWeb.Dataloaders.Druid, as: DruidLoader
  alias Absinthe.Utils, as: AbsintheUtils

  import_types(Absinthe.Type.Custom)

  import_types(__MODULE__.Types.{
    Drilldown,
    LivenessCheck
  })

  import_types(CogyntGraphql.Schema.Scalars.{
    JSON
  })

  def context(ctx) do
    loader =
      Dataloader.new()
      |> Dataloader.add_source(DruidLoader, DruidLoader.data())

    Map.put(ctx, :loader, loader)
  end

  def plugins do
    [Absinthe.Middleware.Dataloader] ++ Absinthe.Plugin.defaults()
  end

  def middleware(middleware, %{identifier: identifier} = field, object) do
    middleware_spec =
      Absinthe.Schema.replace_default(
        middleware,
        {{__MODULE__, :get_value}, identifier},
        field,
        object
      )

    (middleware_spec ++ [ErrorHandler])
    |> Enum.map(&ErrorException.wrap/1)
  end

  def get_value(%{source: source} = res, key) do
    string = key |> Atom.to_string()
    camelized = string |> AbsintheUtils.camelize(lower: true)

    %{
      res
      | state: :resolved,
        value: Map.get(source, key, Map.get(source, string, Map.get(source, camelized)))
    }
  end

  query do
    import_fields(:drilldown_queries)
    import_fields(:liveness_check_queries)
  end
end
