defmodule CogyntWorkstationIngest.Servers.Workers.RedisStreamsConsumerGroupWorker do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.DynamicTaskSupervisor

  alias CogyntWorkstationIngest.Servers.{
    DeploymentTaskMonitor,
    DrilldownTaskMonitor,
    EventDefinitionTaskMonitor
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
                        if not DeploymentTaskMonitor.deployment_task_running?() do
                          DynamicTaskSupervisor.start_child(%{delete_deployment_data: true})
                        end
                      else
                        if reset_drilldown do
                          if not DrilldownTaskMonitor.drilldown_task_running?() do
                            DynamicTaskSupervisor.start_child(%{
                              delete_drilldown_data: delete_drilldown_topics
                            })
                          end
                        end

                        event_definition_ids =
                          Enum.reject(event_definition_ids, fn event_definition_id ->
                            EventDefinitionTaskMonitor.event_definition_task_running?(
                              event_definition_id
                            )
                          end)

                        if length(event_definition_ids) > 0 do
                          DynamicTaskSupervisor.start_child(%{
                            delete_event_definitions_and_topics: %{
                              event_definition_ids: event_definition_ids,
                              hard_delete: false,
                              delete_topics: delete_topics
                            }
                          })
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
