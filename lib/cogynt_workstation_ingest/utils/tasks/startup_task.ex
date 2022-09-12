defmodule CogyntWorkstationIngest.Utils.Tasks.StartUpTask do
  @moduledoc """
  Task to run needed logic for application startup
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper
  alias CogyntWorkstationIngest.Pinot.Controller

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    IO.inspect(node(), label: "NODE NAME")
    event_definitions = EventsContext.query_event_definitions(%{})
    start_event_type_pipelines(event_definitions)
    start_deployment_pipeline()

    if Config.drilldown_enabled?() do
      # DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
      #   Config.template_solutions_topic()
      # )
      Controller.get_schema(Config.template_solution_events_topic())
      |> then(fn
        {:error, {404, _}} ->
          %{
            schemaName: Config.template_solution_events_topic(),
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
          |> Controller.validate_schema()
          |> then(fn
            {:ok, schema} ->
              Controller.create_schema(schema, query: [override: true])

            {:error, error} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Invalid schema definition provided to create #{Config.template_solution_events_topic()} schema."
              )

              {:error, error}
          end)

        response ->
          response
      end)
      |> then(fn
        {:ok, _} ->
          %{
            tableName: Config.template_solution_events_topic(),
            tableType: "REALTIME",
            segmentsConfig: %{
              schemaName: Config.template_solution_events_topic(),
              replication: "1",
              replicasPerPartition: "1",
              minimizeDataMovement: false,
              timeColumnName: "publishedAt"
            },
            tenants: %{
              broker: "DefaultTenant",
              server: "DefaultTenant",
              tagOverrideConfig: %{}
            },
            tableIndexConfig: %{
              loadMode: "MMAP",
              streamConfigs: %{
                streamType: "kafka",
                "stream.kafka.topic.name": Config.template_solution_events_topic(),
                # TODO: MAKE THIS CONFIGURABLE
                "stream.kafka.broker.list": "kafka.cogynt.svc.cluster.local:9071",
                "stream.kafka.consumer.type": "lowlevel",
                "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
                "stream.kafka.consumer.factory.class.name":
                  "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
                "stream.kafka.decoder.class.name":
                  "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
                # TODO: MAKE THIS CONFIGURABLE
                "stream.kafka.decoder.prop.schema.registry.rest.url":
                  "http://schemaregistry.cogynt.svc.cluster.local:8081",
                "realtime.segment.flush.threshold.rows": "0",
                "realtime.segment.flush.threshold.time": "24h",
                "realtime.segment.flush.segment.size": "100M"
              }
            },
            metadata: %{},
            ingestionConfig: %{
              transformConfigs: [
                %{
                  columnName: "eventId",
                  transformFunction: "JSONPATH(event, 'COG_id')"
                },
                %{
                  columnName: "version",
                  transformFunction: "JSONPATH(event, 'COG_version')"
                }
              ]
            },
            isDimTable: false
          }
          |> Controller.validate_table()
          |> then(fn
            {:error, error} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Invalid table config provided to create #{Config.template_solution_events_topic()} table. Error: #{inspect(error)}"
              )

              {:error, error}

            {:ok, %{REALTIME: %{tableName: table_name} = table_config}} ->
              Controller.get_table(Config.template_solution_events_topic(),
                query: [type: "realtime"]
              )
              |> then(fn
                {:ok, %{REALTIME: %{tableName: table_name} = table}} ->
                  Controller.update_table(table_name, table_config)
                  |> case do
                    {:ok, %{status: status}} ->
                      CogyntLogger.info("#{__MODULE__}", status)
                      :ok

                    {:error, error} ->
                      CogyntLogger.error(
                        "#{__MODULE__}",
                        "An error occurred while trying to update the #{table_name} table. Error: #{inspect(error)}"
                      )

                      {:error, error}
                  end

                {:ok, table} when table == %{} ->
                  Controller.create_table(table_config)
                  |> case do
                    {:ok, %{status: status}} ->
                      CogyntLogger.info("#{__MODULE__}", status)
                      :ok

                    {:error, error} ->
                      CogyntLogger.error(
                        "#{__MODULE__}",
                        "An error occurred while trying to create the #{table_name} table. Error: #{inspect(error)}"
                      )

                      {:error, error}
                  end

                {:error, error} ->
                  CogyntLogger.error(
                    "#{__MODULE__}",
                    "An error occurred while trying to create #{table_name} table. Error: #{inspect(error)}"
                  )

                  {:error, error}
              end)
          end)

        {:error, error} ->
          {:error, error}
      end)

      DruidRegistryHelper.start_drilldown_druid_with_registry_lookup(
        Config.template_solution_events_topic()
      )
    end

    ExqHelpers.resubscribe_to_all_queues()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_event_type_pipelines(event_definitions) do
    event_definitions = Enum.filter(event_definitions, &Map.get(&1, :active, false))

    Enum.each(event_definitions, fn event_definition ->
      Redis.publish_async("ingest_channel", %{
        start_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })

      CogyntLogger.info("#{__MODULE__}", "EventPipeline Started for Id: #{event_definition.id}")
    end)
  end

  defp start_deployment_pipeline() do
    Redis.publish_async("ingest_channel", %{start_deployment_pipeline: "deployment"})
  end
end
