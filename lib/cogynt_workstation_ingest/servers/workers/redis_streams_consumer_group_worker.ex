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
                ConsumerStateManager.manage_request(%{
                  update_notifications: field_value
                })

              :delete_notifications ->
                ConsumerStateManager.manage_request(%{
                  delete_notifications: field_value
                })

              :delete_event_definition_events ->
                ConsumerStateManager.manage_request(%{delete_event_definition_events: field_value})

              :dev_delete ->
                case field_value do
                  %{
                    drilldown: %{
                      reset_drilldown: reset_drilldown,
                      delete_drilldown_topics: delete_drilldown_topics
                    },
                    deployment: reset_deployment,
                    event_definitions: %{
                      event_definition_ids: event_definition_ids,
                      delete_topics: delete_topics
                    }
                  } ->
                    try do
                      if reset_deployment do
                        create_job_queue_if_not_exists("DevDelete")
                        Exq.enqueue(Exq, "DevDelete", DeleteDeploymentDataWorker, [])
                      else
                        if reset_drilldown do
                          create_job_queue_if_not_exists("DevDelete")
                          Exq.enqueue(Exq, "DevDelete", DeleteDrilldownDataWorker, [
                            delete_drilldown_topics
                          ])
                        end

                        if length(event_definition_ids) > 0 do
                          create_job_queue_if_not_exists("DevDelete")
                          Exq.enqueue(Exq, "DevDelete", DeleteEventDefinitionsAndTopicsWorker, [
                            %{
                              event_definition_ids: event_definition_ids,
                              hard_delete: false,
                              delete_topics: delete_topics
                            }
                          ])
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

  defp create_job_queue_if_not_exists(queue_name) do
    case Exq.subscriptions(Exq) do
      {:ok, subscriptions} ->
        if !Enum.member?(subscriptions, queue_name) do
          Exq.subscribe(Exq, queue_name, 1)
          CogyntLogger.info("#{__MODULE__}", "Created Queue: #{queue_name}")
        end

        {:ok, queue_name}

      _ ->
        CogyntLogger.error("#{__MODULE__}", "Exq.Api.queues/1 failed to fetch queues")
        {:error, :failed_to_fetch_queues}
    end
  end
end
