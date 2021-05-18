defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutionEvents do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solution_events_topic(),
    use_avro: true,
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(","),
    # brokers: "kafka-qa.cogilitycloud.com:31090",
    dimensions_spec: %{
      dimensions: [
        "id",
        "templateTypeName",
        "templateTypeId",
        "event",
        "aid",
        "assertionName",
        "eventId"
      ]
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
