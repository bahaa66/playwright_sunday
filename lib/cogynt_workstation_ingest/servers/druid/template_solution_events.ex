defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutionEvents do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solution_events_topic(),
    dimensions: [
      "id",
      "template_type_name",
      "template_type_id",
      "event",
      "aid",
      "assertion_name",
      "event.id"
    ],
    flattened_fields: [
      %{
        type: "path",
        name: "event.id",
        expr: "$.event.id"
      }
    ],
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(",")
end
