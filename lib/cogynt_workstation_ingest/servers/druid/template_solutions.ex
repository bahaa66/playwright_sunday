defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutions do
  alias CogyntWorkstationIngest.Config

  use CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor,
    supervisor_id: Config.template_solutions_topic(),
    schema: :avro,
    schema_registry_url: "http://schemaregistry:8081",
    # schema_registry_url: "http://localhost:8089",
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
    }
end
