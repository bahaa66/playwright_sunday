defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutions do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solutions_topic(),
    use_avro: true,
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(","),
    dimensions_spec: %{
      dimensions: [
        "id",
        "templateTypeName",
        "templateTypeId",
        "retracted"
      ]
    },
    # TODO: Change this to use the schema registry url and use "schema_repo" type
    avro_schema: %{
      name: "TemplateSolution",
      namespace: "com.cogility.hcep.generated",
      type: "record",
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
          name: "retracted",
          type: "boolean"
        }
      ]
    }
end
