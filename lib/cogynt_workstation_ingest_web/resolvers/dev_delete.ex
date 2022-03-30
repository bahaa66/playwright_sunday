defmodule CogyntWorkstationIngestWeb.Resolvers.DevDelete do
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntGraphql.Utils.Error

  def delete_data(_, args, _) do
    try do
      # 1) delete drilldown data
      ExqHelpers.create_and_enqueue(
        "DevDelete",
        nil,
        DeleteDrilldownDataWorker,
        Map.get(args, :delete_deployment_topic, false),
        :infinite
      )

      # 2) delete event_definitions data
      EventsContext.list_event_definitions()
      |> Enum.each(fn %{id: event_definition_hash_id} ->
        ExqHelpers.enqueue(
          "DevDelete",
          DeleteEventDefinitionsAndTopicsWorker,
          %{
            event_definition_hash_id: event_definition_hash_id
          }
        )
      end)

      # 3) delete deployment data
      ExqHelpers.enqueue(
        "DevDelete",
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

  def reset_drilldown_data(_, %{delete_topics: delete_topics}, _) do
    try do
      ExqHelpers.create_and_enqueue(
        "DevDelete",
        nil,
        DeleteDrilldownDataWorker,
        delete_topics,
        :infinite
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
      EventsContext.query_event_definitions(%{
        filter: %{event_definition_hash_ids: Map.get(args, :ids)}
      })
      |> Enum.each(fn %{id: event_definition_hash_id} ->
        ExqHelpers.enqueue(
          "DevDelete",
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
