defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutions do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solutions_topic(),
    dimensions: [
      "id",
      "template_type_name",
      "template_type_id",
      "events",
      "outcomes"
    ],
    brokers:
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(",")
end
