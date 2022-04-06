defmodule CogyntWorkstationIngestWeb.Resolvers.DevDelete do
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntGraphql.Utils.Error

  @dev_delete_queue_name "DevDelete"

  def delete_data(_, args, _) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      # 1) delete drilldown data
      ExqHelpers.enqueue(
        @dev_delete_queue_name,
        DeleteDrilldownDataWorker,
        Map.get(args, :delete_deployment_topic, false)
      )

      # 2) delete event_definitions data
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

      # 3) delete deployment data
      ExqHelpers.enqueue(
        @dev_delete_queue_name,
        DeleteDeploymentDataWorker,
        Map.get(args, :delete_deployment_topic, false)
      )

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

  def reset_drilldown_data(_, _, _) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      ExqHelpers.enqueue(
        @dev_delete_queue_name,
        DeleteDrilldownDataWorker,
        false
      )

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

  def delete_event_definitions(_, args, _) do
    try do
      ExqHelpers.create_job_queue_if_not_exists(@dev_delete_queue_name, nil)

      EventsContext.query_event_definitions(%{
        filter: %{event_definition_hash_ids: Map.get(args, :ids)}
      })
      |> Enum.each(fn %{id: event_definition_hash_id} ->
        ExqHelpers.enqueue(
          @dev_delete_queue_name,
          DeleteEventDefinitionsAndTopicsWorker,
          %{
            event_definition_hash_id: event_definition_hash_id
          }
        )
      end)

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
