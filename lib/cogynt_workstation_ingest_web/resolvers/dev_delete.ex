defmodule CogyntWorkstationIngestWeb.Resolvers.DevDelete do
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntGraphql.Utils.Error

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  @dev_delete_queue_name "DevDelete"

  def delete_data(_, args, _) do
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
        # DEBUG
        producer_name =
          (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_hash_id) <> "Pipeline")
          |> String.to_atom()
          |> Broadway.producer_names()
          |> List.first()

        temp = :sys.get_status(producer_name, :infinity)
        IO.inspect(temp, label: "SYS INFO", pretty: true)

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
