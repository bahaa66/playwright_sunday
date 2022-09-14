defmodule CogyntWorkstationIngest.Utils.PinotUtils do
  alias CogyntWorkstationIngest.Config
  alias Pinot.Controller
  alias Pinot.Config, as: PinotConfig

  def create_schema_and_table(table_name, opts \\ []) do
    schema_name = Keyword.get(opts, :schema_name, table_name)
    create_schema(schema_name)
      |> then(fn
        {:ok, %{status: status}} ->
          CogyntLogger.info("#{__MODULE__}", "Pinot schema #{status}")

          create_table(table_name)
          |> then(fn
            {:ok, %{status: status}} ->
              CogyntLogger.info("#{__MODULE__}", status)
              :ok

            {:error, error} ->
              {:error, "An error occurred while creating the #{table_name} Pinot table. Error: #{inspect(error)}"}
          end)

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "An error occurred while creating the #{Config.template_solution_events_topic()} Pinot schema. Error: #{inspect(error)}"
          )

          {:error, "An error occurred while creating the #{Config.template_solution_events_topic()} Pinot schema. Error: #{inspect(error)}"}
      end)
  end

  def create_schema(schema_name) do
    Controller.get_schema(schema_name)
    |> then(fn
      {:error, {404, _}} ->
        schema_config!(schema_name)
        |> Controller.validate_schema()
        |> then(fn
          {:ok, schema} ->
            Controller.create_schema(schema, query: [override: true])

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Invalid schema definition provided to create #{schema_name} schema."
            )

            {:error, error}
        end)

      _response ->
        {:ok, %{status: "#{schema_name} already created."}}
    end)
  end

  def create_table(table_name) do
    table_config!(
      table_name,
      kafka_brokers: PinotConfig.kafka_broker_list(),
      schema_registry_url: PinotConfig.schema_registry_url()
    )
    |> Controller.validate_table()
    |> then(fn
      {:ok, %{REALTIME: %{tableName: table_name} = table_config}} ->
        Controller.get_table(table_name)
        |> then(fn
          {:ok, table} when table == %{} ->
            Controller.create_table(table_config)

          {:ok, %{REALTIME: %{tableName: table_name}}} ->
            Controller.update_table(table_name, table_config)

          response ->
            response
        end)

      response ->
        response
    end)
  end

  @base_segment_config %{
    replication: "1",
    replicasPerPartition: "1",
    minimizeDataMovement: false,
    timeColumnName: "published_at"
  }

  @base_stream_configs %{
    streamType: "kafka",
    "stream.kafka.consumer.type": "lowlevel",
    "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
    "stream.kafka.consumer.factory.class.name":
      "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
    "stream.kafka.decoder.class.name":
      "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
    "realtime.segment.flush.threshold.rows": "0",
    "realtime.segment.flush.threshold.time": "24h",
    "realtime.segment.flush.segment.size": "100M"
  }

  @base_table_index_configs %{
    loadMode: "MMAP"
  }

  @base_template_solution_events_table_config %{
    tableType: "REALTIME",
    tenants: %{
      broker: "DefaultTenant",
      server: "DefaultTenant",
      tagOverrideConfig: %{}
    },
    metadata: %{},
    ingestionConfig: %{
      transformConfigs: [
        %{
          columnName: "solution_id",
          transformFunction: "id"
        },
        %{
          columnName: "template_type_id",
          transformFunction: "templateTypeId"
        },
        %{
          columnName: "template_type_name",
          transformFunction: "templateTypeName"
        },
        %{
          columnName: "published_at",
          transformFunction: "publishedAt"
        },
        %{
          columnName: "event_id",
          transformFunction: "JSONPATH(event, '$.#{Config.id_key()}')"
        },
        %{
          columnName: "version",
          transformFunction: "JSONPATH(event, '$.#{Config.version_key()}')"
        }
      ]
    },
    routing: %{
      instanceSelectorType: "strictReplicaGroup"
    },
    upsertConfig: %{
      mode: "FULL"
    },
    isDimTable: false
  }

  defp table_config!("template_solution_events", opts) do
    schema_name = Keyword.get(opts, :schema_name, "template_solution_events")
    topic = Keyword.get(opts, :topic, "template_solution_events")
    kafka_brokers = Keyword.get(opts, :kafka_brokers)
    schema_registry_url = Keyword.get(opts, :schema_registry_url)

    @base_template_solution_events_table_config
    |> Map.merge(%{tableName: "template_solution_events"})
    |> apply_segment_configs(schema_name)
    |> apply_table_index_configs(topic, kafka_brokers, schema_registry_url)
  end

  defp table_config!(table_name, _),
    do: raise("#{table_name} table config definition not defined in #{__MODULE__}")

  defp apply_segment_configs(table_configs, schema_name) do
    segment_configs = @base_segment_config |> Map.merge(%{schemaName: schema_name})
    Map.put(table_configs, :segmentsConfig, segment_configs)
  end

  defp apply_table_index_configs(table_configs, topic, kafka_brokers, schema_registry_url) do
    table_index_configs =
      @base_table_index_configs
      |> apply_stream_configs(topic, kafka_brokers, schema_registry_url)

    Map.put(table_configs, :tableIndexConfig, table_index_configs)
  end

  defp apply_stream_configs(table_index_configs, topic, kafka_brokers, schema_registry_url) do
    stream_configs =
      @base_stream_configs
      |> Map.merge(%{
        "stream.kafka.topic.name": topic,
        "stream.kafka.broker.list": kafka_brokers,
        "stream.kafka.decoder.prop.schema.registry.rest.url": schema_registry_url
      })

    Map.put(table_index_configs, :streamConfigs, stream_configs)
  end

  @template_solution_events_schema_config %{
    primaryKeyColumns: ["id", "event_id", "aid"],
    dimensionFieldSpecs: [
      %{
        name: "solution_id",
        dataType: "STRING"
      },
      %{
        name: "id",
        dataType: "STRING"
      },
      %{
        name: "event",
        dataType: "JSON"
      },
      %{
        name: "event_id",
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
        name: "template_type_id",
        dataType: "STRING"
      },
      %{
        name: "template_type_name",
        dataType: "STRING"
      }
    ],
    dateTimeFieldSpecs: [
      %{
        name: "published_at",
        dataType: "TIMESTAMP",
        format: "1:MILLISECONDS:EPOCH",
        granularity: "1:MILLISECONDS"
      }
    ]
  }

  defp schema_config!("template_solution_events"),
    do:
      @template_solution_events_schema_config
      |> Map.merge(%{schemaName: "template_solution_events"})

  defp schema_config!(schema_name),
    do: raise("#{schema_name} schema config definition not defined in #{__MODULE__}")
end
