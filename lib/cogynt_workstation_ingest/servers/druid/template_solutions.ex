defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutions do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solutions_topic(),
    schema: :avro,
    schema_registry_url: "http://schemaregistry:8081",
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(","),
    timestampSpec: %{
      column: "_timestamp",
      format: "auto",
      missingValue: "1970-01-01T00:00:00Z"
    },
    dimensionsSpec: %{
      dimensions: [
        %{
          name: "templateTypeName",
          type: "string"
        },
        %{
          name: "templateTypeId",
          type: "string"
        },
        %{
          name: "id",
          type: "string"
        },
        %{
          name: "retracted",
          type: "boolean"
        }
      ],
      dimensionExclusions: []
    }
end
