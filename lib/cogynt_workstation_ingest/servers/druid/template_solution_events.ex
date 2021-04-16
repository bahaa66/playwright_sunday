defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutionEvents do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solution_events_topic(),
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
    }
end
