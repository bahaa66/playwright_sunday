defmodule CogyntWorkstationIngestWeb.Schema.Types.Drilldown do
  use Absinthe.Schema.Notation

  # Example data
  @drilldowns %{
    "foo" => %{
      attributes: %{
        __time: "1970-01-01T00:00:00.000Z",
        key: "ebc306f5-c8ae-39f4-9ba7-da6f3a64f546",
        retracted: false,
        template_type_id: "f28c0e6c-7bc6-11ea-bd3f-acde48001122",
        template_type_name: "Suspected Mule Acction"
      }
    }
  }

  object :drilldown_queries do
    field :drilldown, :drilldown do
      arg(:id, non_null(:id))

      resolve(fn %{id: drilldown_id}, _ ->
        {:ok, @drilldowns[drilldown_id]}
      end)
    end
  end

  object :drilldown do
    field :attributes, non_null(:drilldown_attributes)
  end

  object :drilldown_attributes do
    field :_time, :datetime
    field :key, :id
    field :retracted, :boolean
    field :template_type_id, :id
    field :template_type_name, :string
  end
end
