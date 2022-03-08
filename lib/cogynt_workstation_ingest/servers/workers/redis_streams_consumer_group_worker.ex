defmodule CogyntWorkstationIngest.Servers.Workers.RedisStreamsConsumerGroupWorker do
  @moduledoc """
  Worker module that fetches messages of a Redis ConsumerGroup for the `itw` stream.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers
  alias CogyntWorkstationIngest.Events.EventsContext

  @count 1

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    Process.send_after(__MODULE__, :fetch_tasks, Config.ingest_task_worker_timer())
    {:ok, %{}}
  end

  @impl true
  def handle_info(:fetch_tasks, state) do
    case Redis.stream_read_group("itw", "cogynt-ws-ingest-otp", @count) do
      {:ok, stream_results} ->
        Enum.each(stream_results, fn [message_id, message_fields] ->
          Enum.each(message_fields, fn {field_name, field_value} ->
            case field_name do
              :backfill_notifications ->
                ConsumerStateManager.manage_request(%{backfill_notifications: field_value})

              :update_notifications ->
                ConsumerStateManager.manage_request(%{update_notifications: field_value})

              :delete_notifications ->
                ConsumerStateManager.manage_request(%{
                  delete_notifications: field_value
                })

              :dev_delete ->
                case field_value do
                  %{
                    drilldown: %{
                      reset_drilldown: reset_drilldown,
                      delete_drilldown_topics: delete_drilldown_topics
                    },
                    deployment: %{
                      reset_deployment: reset_deployment,
                      delete_topics: delete_topics_for_deployments
                    },
                    event_definitions: %{
                      event_definition_hash_ids: event_definition_hash_ids,
                      delete_topics: delete_topics
                    }
                  } ->
                    try do
                      if reset_deployment do
                        # 1) delete drilldown data
                        ExqHelpers.create_and_enqueue(
                          "DevDelete",
                          nil,
                          DeleteDrilldownDataWorker,
                          delete_topics_for_deployments,
                          :infinite
                        )

                        # 2) delete event_definitions data
                        EventsContext.list_event_definitions()
                        |> Enum.group_by(fn ed -> ed.id end)
                        |> Map.keys()
                        |> Enum.each(fn event_definition_hash_id ->
                          ExqHelpers.enqueue(
                            "DevDelete",
                            DeleteEventDefinitionsAndTopicsWorker,
                            %{
                              event_definition_hash_id: event_definition_hash_id,
                              delete_topics: delete_topics_for_deployments
                            }
                          )
                        end)

                        # 3) delete deployment data
                        ExqHelpers.enqueue(
                          "DevDelete",
                          DeleteDeploymentDataWorker,
                          delete_topics_for_deployments
                        )
                      else
                        if reset_drilldown do
                          ExqHelpers.create_and_enqueue(
                            "DevDelete",
                            nil,
                            DeleteDrilldownDataWorker,
                            delete_drilldown_topics,
                            :infinite
                          )
                        end

                        if length(event_definition_hash_ids) > 0 do
                          Enum.each(event_definition_hash_ids, fn event_definition_hash_id ->
                            ExqHelpers.create_and_enqueue(
                              "DevDelete",
                              nil,
                              DeleteEventDefinitionsAndTopicsWorker,
                              %{
                                event_definition_hash_id: event_definition_hash_id,
                                delete_topics: delete_topics
                              },
                              :infinite
                            )
                          end)
                        end
                      end
                    rescue
                      error ->
                        CogyntLogger.error(
                          "#{__MODULE__}",
                          "dev_delete failed with error: #{inspect(error, pretty: true)}"
                        )
                    end

                  _ ->
                    CogyntLogger.error(
                      "#{__MODULE__}",
                      "Invalid params passed to dev_delete. #{inspect(field_value, pretty: true)}"
                    )
                end

              _ ->
                nil
            end
          end)

          # ACK the message as processed
          Redis.stream_ack_message("itw", message_id)
        end)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to read from redis consumer group. reason: #{inspect(reason)}"
        )
    end

    Process.send_after(__MODULE__, :fetch_tasks, Config.ingest_task_worker_timer())
    {:noreply, state}
  end
end
