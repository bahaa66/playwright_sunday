defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutionEvents do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solution_events_topic(),
    use_avro: true,
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(","),
    dimensions_spec: %{
      dimensions: [
        "id",
        "template_type_name",
        "template_type_id",
        "event",
        "aid",
        "assertion_name",
        "event_id"
      ]
    },
    io_config: %{
      type: "json",
      flattenSpec: %{
        fields: [
          %{
            type: "path",
            name: "event_id",
            expr: "$.event.id"
          },
          %{
            type: "jq",
            name: "event",
            expr: ".event | tojson"
          }
        ]
      }
    },
    avro_schema: %{
      fields: [
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
      name: "SolutionEvent",
      namespace: "com.cogility.hcep.generated",
      type: "record"
    }
end
