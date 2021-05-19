defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutionEvents do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solution_events_topic(),
    schema: :avro,
    schema_registry_url: "http://schemaregistry:8081",
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(","),
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
          name: "aid",
          type: [
            "null",
            "string"
          ]
        },
        %{
          name: "assertionName",
          type: [
            "null",
            "string"
          ]
        },
        %{
          name: "event",
          type: "string"
        }
      ],
      dimensionExclusions: []
    },
    timestampSpec: %{
      column: "timestamp",
      format: "auto"
    }
end
