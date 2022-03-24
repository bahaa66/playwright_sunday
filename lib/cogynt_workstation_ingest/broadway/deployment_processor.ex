defmodule CogyntWorkstationIngest.Broadway.DeploymentProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DeploymentPipeline.
  """
  alias CogyntWorkstationIngest.DataSources.DataSourcesContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper
  alias Broadway.Message

  @deployment_target_hash_constant "00000000-0000-0000-0000-000000000000"

  @doc """
  process_deployment_message/1
  """
  def process_deployment_message(%Message{data: nil} = message) do
    CogyntLogger.error(
      "#{__MODULE__}",
      "process_deployment_message/1 Message data is nil. No data to process"
    )

    message
  end

  def process_deployment_message(
        %Message{data: %{deployment_message: deployment_message}} = message
      ) do
    case Map.get(deployment_message, :version) do
      # Use the Authoring 2.0 deployment message schemas
      "2.0" ->
        case Map.get(deployment_message, :objectType, nil) do
          "event_type" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for objectType: event_type, version: 2.0, id: #{deployment_message.id}"
            )

            process_event_type_object_v2(deployment_message)
            message

          "deployment" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for objectType: deployment, version: 2.0, id: #{deployment_message.id}"
            )

            process_data_sources_v2(deployment_message)
            message

          "user_data_schema" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for objectType: user_data_schema, version: 2.0, id: #{deployment_message.id}"
            )

            process_user_data_schema_object(deployment_message)
            message

          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_deployment_message/1 `objectType` key is missing from Deployment Stream message. #{inspect(deployment_message, pretty: true)}"
            )

            message

          other ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_deployment_message/1 `objectType` key: #{other} not yet supported."
            )

            message
        end

      # Use the Authoring 1.0 deployment message schemas
      # *** These can be deprecated once Authoring 1.0 is no longer supported ***
      _ ->
        case Map.get(deployment_message, :object_type, nil) do
          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "`object_type` key is missing from Deployment Stream message. #{inspect(deployment_message, pretty: true)}"
            )

            message

          "event_type" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for object_type: event_type, version: 1.0, id: #{deployment_message.id}"
            )

            process_event_type_object(deployment_message)
            message

          "deployment" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for object_type: deployment, version: 1.0, id: #{deployment_message.id}"
            )

            process_data_sources(deployment_message)
            message

          other ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_deployment_message/1 `object_type` key: #{other} not yet supported."
            )

            message
        end
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp process_data_sources(deployment_message) do
    Enum.each(
      deployment_message.data_sources,
      fn data_source ->
        case data_source.kind == "kafka" do
          true ->
            primary_key =
              UUID.uuid5(
                @deployment_target_hash_constant,
                to_string(data_source.spec.data_source_id)
              )

            Map.put(deployment_message, :id, primary_key)
            |> Map.put(:type, data_source.kind)
            |> Map.put(:data_source_name, data_source.spec.name)
            |> Map.put(:connect_string, data_source.spec.brokers)
            |> DataSourcesContext.upsert_datasource()

          false ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_data_sources/1 data_source type: #{inspect(data_source.kind)} not supported "
            )
        end
      end
    )
  end

  defp process_data_sources_v2(deployment_message) do
    Enum.each(
      deployment_message.dataSources,
      fn data_source ->
        case data_source.type == "kafka" do
          true ->
            Map.put(deployment_message, :id, data_source.dataSourceId)
            |> Map.put(:type, data_source.type)
            |> Map.put(:data_source_name, data_source.name)
            |> Map.put(:connect_string, data_source.connectString)
            |> DataSourcesContext.upsert_datasource()

          false ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_data_sources_v2/1 data_source type: #{inspect(data_source.type)} not supported "
            )
        end
      end
    )
  end

  defp process_event_type_object(deployment_message) do
    data_source_id_uuid =
      UUID.uuid5(@deployment_target_hash_constant, to_string(deployment_message.data_source_id))

    primary_key = UUID.uuid5(deployment_message.id, data_source_id_uuid)

    Map.put(deployment_message, :event_definition_id, deployment_message.id)
    |> Map.put(:id, primary_key)
    |> Map.put(:data_source_id, data_source_id_uuid)
    |> Map.put(:project_name, "COG_Project_Placeholder")
    |> Map.put(:topic, deployment_message.filter)
    |> Map.put(:event_definition_details_id, primary_key)
    |> Map.put(:title, deployment_message.name)
    |> Map.put(
      :manual_actions,
      Map.get(deployment_message, :manualActions, nil)
    )
    |> Map.put_new_lazy(:event_type, fn ->
      if is_nil(deployment_message.dsType) do
        :none
      else
        deployment_message.dsType
      end
    end)
    |> EventsContext.upsert_event_definition()
    |> case do
      {:ok, event_definition} ->
        with name <- ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id),
             true <- name != "" do
          DruidRegistryHelper.update_druid_with_registry_lookup(event_definition)
        end

      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to upsert EventDefinition for DeploymentProcessor. Error: #{inspect(error)}"
        )
    end
  end

  defp process_event_type_object_v2(deployment_message) do
    primary_key = UUID.uuid5(deployment_message.id, deployment_message.dataSourceId)

    Map.put(deployment_message, :event_definition_id, deployment_message.id)
    |> Map.put(:id, primary_key)
    |> Map.put(:data_source_id, deployment_message.dataSourceId)
    |> Map.put(:project_name, deployment_message.projectName)
    |> Map.put(:topic, deployment_message.source.topic)
    |> Map.put(:title, deployment_message.name)
    |> Map.put(
      :manual_actions,
      Map.get(deployment_message, :manualActions, nil)
    )
    |> Map.put(
      :event_type,
      Map.get(deployment_message, :linkAnalysisType, :none)
    )
    |> Map.put(:event_definition_details_id, deployment_message.userDataSchemaId)
    |> EventsContext.upsert_event_definition_v2()
    |> case do
      {:ok, event_definition} ->
        with name <- ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id),
             true <- name != "" do
          DruidRegistryHelper.update_druid_with_registry_lookup(event_definition)
        end

      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to upsert EventDefinition for DeploymentProcessor. Error: #{inspect(error)}"
        )
    end
  end

  defp process_user_data_schema_object(deployment_message) do
    # 1) if any PG record exists with id. Remove all records for it
    EventsContext.hard_delete_event_definition_details(deployment_message.id)
    # 2) insert new user data schema into PG
    EventsContext.process_event_definition_detail_fields_v2(
      deployment_message.id,
      deployment_message.fields
    )
    |> EventsContext.insert_all_event_details()
  end
end
