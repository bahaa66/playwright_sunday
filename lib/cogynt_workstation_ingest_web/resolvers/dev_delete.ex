defmodule CogyntWorkstationIngestWeb.Resolvers.DevDelete do
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntGraphql.Utils.Error
  alias Kafka.Producer.Audit
  alias Models.Structs.AuditObject
  alias Models.Enums.{ChangeType, ObjectType, AuditAction}

  @dev_delete_queue_name "DevDelete"

  def delete_data(_, args, %{context: %{client_ip: client_ip, current_user: %{id: user_id}}}) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      # 1) delete event_definitions data
      EventsContext.list_event_definitions()
      |> Enum.each(fn %{id: event_definition_hash_id} ->
        ExqHelpers.enqueue(
          @dev_delete_queue_name,
          DeleteEventDefinitionsAndTopicsWorker,
          %{
            event_definition_hash_id: event_definition_hash_id
          }
        )
      end)

      # 2) delete deployment data
      ExqHelpers.enqueue(
        @dev_delete_queue_name,
        DeleteDeploymentDataWorker,
        Map.get(args, :delete_deployment_topic, false)
      )

      Audit.publish_async(%AuditObject{
        object_type: ObjectType.WorkstationData.value(),
        change_type: ChangeType.Delete.value(),
        object_reference: Ecto.UUID.generate(),
        user_id: user_id,
        audit_action: AuditAction.DeleteAllData.value(),
        object_metadata: %{
          description: "Delete all Workstation data was triggered",
          delete_deployment_topic: Map.get(args, :delete_deployment_topic, false)
        },
        client_ip: client_ip
      })

      {:ok, %{status: "ok"}}
    rescue
      error ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while trying to queue dev delete jobs.",
           code: :internal_server_error,
           details: "An internal server occurred while trying to queue dev delete jobs.",
           original_error: error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}
    end
  end

  def reset_drilldown_data(_, _, %{context: %{client_ip: client_ip, current_user: %{id: user_id}}}) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      ExqHelpers.enqueue(
        @dev_delete_queue_name,
        DeleteDrilldownDataWorker,
        false
      )

      Audit.publish_async(%AuditObject{
        object_type: ObjectType.WorkstationData.value(),
        change_type: ChangeType.Delete.value(),
        object_reference: Ecto.UUID.generate(),
        user_id: user_id,
        audit_action: AuditAction.ResetDrilldownData.value(),
        object_metadata: %{
          description: "Delete/Resetting all Drilldown data was triggered"
        },
        client_ip: client_ip
      })

      {:ok, %{status: "ok"}}
    rescue
      error ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while trying to queue dev delete jobs.",
           code: :internal_server_error,
           details: "An internal server occurred while trying to queue dev delete jobs.",
           original_error: error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}
    end
  end

  def delete_event_definitions(_, args, %{
        context: %{client_ip: client_ip, current_user: %{id: user_id}}
      }) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      event_definitions =
        EventsContext.query_event_definitions(%{
          filter: %{event_definition_hash_ids: Map.get(args, :ids)}
        })

      event_definitions
      |> Enum.each(fn %{id: event_definition_hash_id} ->
        ExqHelpers.enqueue(
          @dev_delete_queue_name,
          DeleteEventDefinitionsAndTopicsWorker,
          %{
            event_definition_hash_id: event_definition_hash_id
          }
        )
      end)

      event_definition_titles =
        Enum.map(event_definitions, fn event_definition -> event_definition.title end)

      Audit.publish_async(%AuditObject{
        object_type: ObjectType.WorkstationData.value(),
        change_type: ChangeType.Delete.value(),
        object_reference: Ecto.UUID.generate(),
        user_id: user_id,
        audit_action: AuditAction.DeleteEventDefinitionData.value(),
        object_metadata: %{
          description: "Delete data for specific EventDefinitions was triggered",
          event_definitions: event_definition_titles
        },
        client_ip: client_ip
      })

      {:ok, %{status: "ok"}}
    rescue
      error ->
        {:error,
         Error.new(%{
           message: "An internal server occurred while trying to queue dev delete jobs.",
           code: :internal_server_error,
           details: "An internal server occurred while trying to queue dev delete jobs.",
           original_error: error,
           module: "#{__MODULE__} line: #{__ENV__.line}"
         })}
    end
  end
end
