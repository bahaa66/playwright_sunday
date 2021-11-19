defmodule CogyntWorkstationIngest.Broadway.DeploymentProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DeploymentPipeline.
  """
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.DataSources.DataSourcesContext
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper
  alias Models.Deployments.Deployment
  alias Models.Enums.DeploymentStatusType
  alias Models.Events.EventDefinition
  alias Broadway.Message

  @deployment_target_hash_constant "INT_TO_UUID"

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
              "Received deployment message for objectType: event_type, version: 2.0, id: #{
                deployment_message.id
              }"
            )

            process_event_type_object_v2(deployment_message)
            message

          "deployment" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for objectType: deployment, version: 2.0, id: #{
                deployment_message.id
              }"
            )

            process_deployment_object_v2(deployment_message)
            process_data_sources_v2(deployment_message)
            message

          "user_data_schema" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for objectType: user_data_schema, version: 2.0, id: #{
                deployment_message.id
              }"
            )

            process_user_data_schema_object(deployment_message)
            message

          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_deployment_message/1 `objectType` key is missing from Deployment Stream message. #{
                inspect(deployment_message, pretty: true)
              }"
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
              "`object_type` key is missing from Deployment Stream message. #{
                inspect(deployment_message, pretty: true)
              }"
            )

            message

          "event_type" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for object_type: event_type, version: 1.0, id: #{
                deployment_message.id
              }"
            )

            process_event_type_object(deployment_message)
            message

          "deployment" ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Received deployment message for object_type: deployment, version: 1.0, id: #{
                deployment_message.id
              }"
            )

            process_deployment_object(deployment_message)
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
  defp process_deployment_object(deployment_message) do
    # Upsert Deployments
    {:ok, %Deployment{} = _deployment} =
      Map.put(deployment_message, :version, to_string(deployment_message.version))
      |> DeploymentsContext.upsert_deployment()

    # Fetch all event_definitions that exists and are assosciated with
    # the deployment_id
    EventsContext.query_event_definitions(
      filter: %{
        deployment_id: deployment_message.id
      }
    )
    # If any of these event_definition_id are not in the list of event_definition_ids
    # passed in the deployment message, mark them as inactive and shut off the consumer.
    # If they are in the list make sure to update the DeploymentStatus if it needs to be changed
    |> Enum.each(fn %EventDefinition{
                      deployment_status: deployment_status,
                      event_definition_id: event_definition_id
                    } = current_event_definition ->
      case Enum.member?(
             deployment_message.event_type_ids,
             event_definition_id
           ) do
        true ->
          if deployment_status == DeploymentStatusType.status()[:inactive] or
               deployment_status == DeploymentStatusType.status()[:not_deployed] do
            EventsContext.update_event_definition(current_event_definition, %{
              deployment_status: DeploymentStatusType.status()[:active]
            })
          end

        false ->
          EventsContext.update_event_definition(current_event_definition, %{
            active: false,
            deployment_status: DeploymentStatusType.status()[:inactive]
          })

          Redis.publish_async("ingest_channel", %{
            stop_consumer:
              EventsContext.remove_event_definition_virtual_fields(current_event_definition)
          })
      end
    end)
  end

  defp process_deployment_object_v2(deployment_message) do
    # Upsert Deployments
    # Temp override of the ID value with a backwards compatible v1DeploymentId field that is an INT
    {:ok, %Deployment{} = _deployment} =
      Map.put(deployment_message, :id, deployment_message.v1DeploymentId)
      |> Map.put(:event_type_ids, deployment_message.eventTypeIds)
      |> Map.put(:data_sources, deployment_message.dataSources)
      |> DeploymentsContext.upsert_deployment()

    # Fetch all event_definitions that exists and are assosciated with
    # the deployment_id
    EventsContext.query_event_definitions(
      filter: %{
        deployment_id: deployment_message.v1DeploymentId
      }
    )
    # If any of these event_definition_id are not in the list of event_definition_ids
    # passed in the deployment message, mark them as inactive and shut off the consumer.
    # If they are in the list make sure to update the DeploymentStatus if it needs to be changed
    |> Enum.each(fn %EventDefinition{
                      deployment_status: deployment_status,
                      event_definition_id: event_definition_id
                    } = current_event_definition ->
      case Enum.member?(
             deployment_message.eventTypeIds,
             event_definition_id
           ) do
        true ->
          if deployment_status == DeploymentStatusType.status()[:inactive] or
               deployment_status == DeploymentStatusType.status()[:not_deployed] do
            EventsContext.update_event_definition(current_event_definition, %{
              deployment_status: DeploymentStatusType.status()[:active]
            })
          end

        false ->
          EventsContext.update_event_definition(current_event_definition, %{
            active: false,
            deployment_status: DeploymentStatusType.status()[:inactive]
          })

          Redis.publish_async("ingest_channel", %{
            stop_consumer:
              EventsContext.remove_event_definition_virtual_fields(current_event_definition)
          })
      end
    end)
  end

  defp process_data_sources(deployment_message) do
    IO.inspect(deployment_message, label: "MSG for DataSources")

    Enum.each(
      deployment_message.data_sources,
      fn data_source ->
        case data_source["type"] == "kafka" do
          true ->
            primary_key =
              UUID.uuid5(data_source.deployment_target_id, @deployment_target_hash_constant)

            Map.put(deployment_message, :id, primary_key)
            |> Map.put(:data_source_name, data_source.name)
            |> Map.put(:connect_string, data_source.brokers)
            |> DataSourcesContext.upsert_datasource()

          false ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_data_sources_v2/1 data_source type: #{inspect(data_source["type"])} not supported "
            )
        end
      end
    )
  end

  defp process_data_sources_v2(deployment_message) do
    IO.inspect(deployment_message, label: "MSG for DataSources")

    Enum.each(
      deployment_message.dataSources,
      fn data_source ->
        # TODO: verify what the data_source object looks like. To see if keys are atoms or strings
        IO.inspect(data_source, label: "Authoring 2 data_source object")

        case data_source["type"] == "kafka" do
          true ->
            Map.put(deployment_message, :id, data_source.deploymentTargetId)
            |> Map.put(:data_source_name, data_source.name)
            |> Map.put(:connect_string, data_source.connectString)
            |> DataSourcesContext.upsert_datasource()

          false ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "process_data_sources_v2/1 data_source type: #{inspect(data_source["type"])} not supported "
            )
        end
      end
    )
  end

  defp process_event_type_object(deployment_message) do
    deployment_target_uuid =
      UUID.uuid5(deployment_message.deployment_target_id, @deployment_target_hash_constant)

    primary_key = UUID.uuid5(deployment_message.id, deployment_target_uuid)

    Map.put(deployment_message, :event_definition_id, deployment_message.id)
    |> Map.put(:id, primary_key)
    |> Map.put(:data_source_id, deployment_target_uuid)
    |> Map.put(:project_name, "COG_Project_Placeholder")
    |> Map.put(:topic, deployment_message.filter)
    |> Map.put(:event_definition_details_id, deployment_message.id)
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
          DruidRegistryHelper.update_druid_with_registry_lookup(name, event_definition)
        end

      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to upsert EventDefinition for DeploymentProcessor. Error: #{inspect(error)}"
        )
    end
  end

  defp process_event_type_object_v2(deployment_message) do
    primary_key = UUID.uuid5(deployment_message.id, deployment_message.deploymentTargetId)

    Map.put(deployment_message, :event_definition_id, deployment_message.id)
    |> Map.put(:id, primary_key)
    |> Map.put(:data_source_id, deployment_message.deploymentTargetId)
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
    |> Map.put(:deployment_id, deployment_message.v1DeploymentId)
    |> EventsContext.upsert_event_definition_v2()
    |> case do
      {:ok, event_definition} ->
        with name <- ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id),
             true <- name != "" do
          DruidRegistryHelper.update_druid_with_registry_lookup(name, event_definition)
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
