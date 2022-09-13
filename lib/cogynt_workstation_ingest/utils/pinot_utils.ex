defmodule CogyntWorkstationIngest.Utils.PinotUtils do
  @template_solution_events_schema_config %{
    primaryKeyColumns: ["id", "eventId", "aid"],
    dimensionFieldSpecs: [
      %{
        name: "id",
        dataType: "STRING"
      },
      %{
        name: "event",
        dataType: "JSON"
      },
      %{
        name: "eventId",
        dataType: "STRING"
      },
      %{
        name: "version",
        dataType: "LONG"
      },
      %{
        name: "aid",
        dataType: "STRING"
      },
      %{
        name: "templateTypeId",
        dataType: "STRING"
      },
      %{
        name: "templateTypeName",
        dataType: "STRING"
      }
    ],
    dateTimeFieldSpecs: [
      %{
        name: "publishedAt",
        dataType: "TIMESTAMP",
        format: "1:MILLISECONDS:EPOCH",
        granularity: "1:MILLISECONDS"
      }
    ]
  }

  def schema_config!("template_solution_events"), do: @template_solution_events_schema_config |> Map.merge(%{schemaName: "template_solution_events"})

  def schema_config!(schema_name), do: raise "#{schema_name} config definition not defined in #{__MODULE__}"
end
