defmodule CogyntWorkstationIngest.Utils.ConsumerStateManager do
  @moduledoc """
  Genserver that keeps track of the State of each Consumer. Acts as a FSM
  that will move ConsumerStatus from one state to another
  """
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.{ConsumerMonitor, EventDefinitionTaskMonitor}
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    BackfillNotificationsWorker,
    UpdateNotificationsWorker,
    DeleteNotificationsWorker,
    DeleteEventDefinitionEventsWorker
  }

  alias Models.Enums.ConsumerStatusTypeEnum

  @default_state %{
    topic: nil,
    status: ConsumerStatusTypeEnum.status()[:unknown],
    prev_status: nil
  }

  @doc """
  creates and or updates the consumer state in the Redis hashkey cs:
  """
  def upsert_consumer_state(event_definition_id, opts) do
    status = Keyword.get(opts, :status, nil)
    topic = Keyword.get(opts, :topic, nil)
    prev_status = Keyword.get(opts, :prev_status, nil)

    case Redis.key_exists?("cs:#{event_definition_id}") do
      {:ok, false} ->
        # create consumer state record
        consumer_state = %{
          status: status,
          topic: topic,
          prev_status: prev_status
        }

        Redis.hash_set_async("cs:#{event_definition_id}", "cs", consumer_state)

        CogyntLogger.info(
          "#{__MODULE__}",
          "New Consumer State created for EventDefinitionId: #{event_definition_id}, #{
            inspect(consumer_state, pretty: true)
          }"
        )

        Redis.publish_async("consumer_state_subscription", %{
          id: event_definition_id,
          topic: topic,
          status: status
        })

        {:ok, :success}

      {:ok, true} ->
        # update consumer state record
        {:ok, consumer_state} = get_consumer_state(event_definition_id)

        consumer_state =
          cond do
            !is_nil(status) and is_nil(prev_status) == true ->
              current_status = consumer_state.status

              consumer_state
              |> Map.put(:status, status)
              |> Map.put(:prev_status, current_status)

            !is_nil(status) and !is_nil(prev_status) == true ->
              consumer_state
              |> Map.put(:status, status)
              |> Map.put(:prev_status, prev_status)

            is_nil(status) and !is_nil(prev_status) == true ->
              consumer_state
              |> Map.put(:prev_status, prev_status)

            is_nil(status) and is_nil(prev_status) == true ->
              consumer_state

            true ->
              consumer_state
          end

        consumer_state =
          if !is_nil(topic) do
            Map.put(consumer_state, :topic, topic)
          else
            consumer_state
          end

        Redis.hash_set_async("cs:#{event_definition_id}", "cs", consumer_state)

        CogyntLogger.info(
          "#{__MODULE__}",
          "Consumer State updated for EventDefinitionId: #{event_definition_id}, #{
            inspect(consumer_state, pretty: true)
          }"
        )

        Redis.publish_async("consumer_state_subscription", %{
          id: event_definition_id,
          topic: consumer_state.topic,
          status: consumer_state.status
        })

        {:ok, :success}
    end
  end

  @doc """
  fetches the consumer_state map stored in the Redis hashkey cs:
  """
  def get_consumer_state(event_definition_id) do
    case Redis.hash_get("cs:#{event_definition_id}", "cs") do
      {:ok, nil} ->
        {:ok, @default_state}

      {:ok, consumer_state} ->
        {:ok, consumer_state}

      {:error, _} ->
        {:error, @default_state}
    end
  end

  @doc """
  removes all redis keys that are associated with the given event_definition_id
  """
  def remove_consumer_state(event_definition_id, opts \\ []) do
    hard_delete_event_definition = Keyword.get(opts, :hard_delete_event_definition, false)

    for x <- ["fem", "emi", "cs"], do: Redis.key_delete("#{x}:#{event_definition_id}")

    Redis.hash_delete("ecgid", "EventDefinition-#{event_definition_id}")
    Redis.hash_delete("ts", event_definition_id)
    Redis.hash_delete("crw", event_definition_id)

    if hard_delete_event_definition do
      Redis.hash_delete("ed", event_definition_id)
    end
  end

  def manage_request(args) do
    %{response: _response} =
      Enum.reduce(args, %{}, fn
        {:start_consumer, event_definition}, _acc ->
          start_consumer(event_definition)

        {:stop_consumer, event_definition}, _acc ->
          stop_consumer(event_definition)

        {:backfill_notifications, notification_setting_id}, _acc ->
          backfill_notifications(notification_setting_id)

        {:update_notifications, notification_setting_id}, _acc ->
          update_notifications(notification_setting_id)

        {:delete_notifications, notification_setting_id}, _acc ->
          delete_notifications(notification_setting_id)

        {:delete_event_definition_events, event_definition_id}, _acc ->
          delete_events(event_definition_id)

        {:handle_unknown_status, event_definition_id}, _acc ->
          handle_unknown_status(event_definition_id)

        _, _ ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Invalid arguments passed to manage_request/1. #{inspect(args, pretty: true)}"
          )
      end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_consumer(event_definition) do
    try do
      case EventDefinitionTaskMonitor.event_definition_task_running?(event_definition.id) do
        false ->
          {:ok, consumer_state} = get_consumer_state(event_definition.id)

          cond do
            consumer_state.status == ConsumerStatusTypeEnum.status()[:running] ->
              case EventPipeline.event_pipeline_running?(event_definition.id) do
                true ->
                  %{response: {:ok, consumer_state.status}}

                false ->
                  case ConsumerGroupSupervisor.start_child(event_definition) do
                    {:error, nil} ->
                      Redis.hash_set_async("crw", event_definition.id, "et")

                      upsert_consumer_state(
                        event_definition.id,
                        topic: event_definition.topic,
                        status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist],
                        prev_status: consumer_state.status
                      )

                      %{response: {:ok, ConsumerStatusTypeEnum.status()[:topic_does_not_exist]}}

                    {:error, {:already_started, _pid}} ->
                      upsert_consumer_state(
                        event_definition.id,
                        topic: event_definition.topic,
                        status: ConsumerStatusTypeEnum.status()[:running],
                        prev_status: consumer_state.status
                      )

                      %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}

                    {:ok, pid} ->
                      ConsumerMonitor.monitor(
                        pid,
                        event_definition.id,
                        event_definition.topic
                      )

                      upsert_consumer_state(
                        event_definition.id,
                        topic: event_definition.topic,
                        status: ConsumerStatusTypeEnum.status()[:running],
                        prev_status: consumer_state.status
                      )

                      %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}
                  end
              end

            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
              consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
                consumer_state.status ==
                  ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: consumer_state.status,
                prev_status: ConsumerStatusTypeEnum.status()[:running]
              )

              %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}

            consumer_state.status == ConsumerStatusTypeEnum.status()[:topic_does_not_exist] ->
              Redis.hash_set_async("crw", event_definition.id, "et")
              %{response: {:ok, consumer_state.status}}

            true ->
              case ConsumerGroupSupervisor.start_child(event_definition) do
                {:error, nil} ->
                  Redis.hash_set_async("crw", event_definition.id, "et")

                  upsert_consumer_state(
                    event_definition.id,
                    topic: event_definition.topic,
                    status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist],
                    prev_status: consumer_state.status
                  )

                  %{response: {:ok, ConsumerStatusTypeEnum.status()[:topic_does_not_exist]}}

                {:error, {:already_started, _pid}} ->
                  upsert_consumer_state(
                    event_definition.id,
                    topic: event_definition.topic,
                    status: ConsumerStatusTypeEnum.status()[:running],
                    prev_status: consumer_state.status
                  )

                  %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}

                {:ok, pid} ->
                  ConsumerMonitor.monitor(
                    pid,
                    event_definition.id,
                    event_definition.topic
                  )

                  upsert_consumer_state(
                    event_definition.id,
                    topic: event_definition.topic,
                    status: ConsumerStatusTypeEnum.status()[:running],
                    prev_status: consumer_state.status
                  )

                  %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}
              end
          end

        true ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to run start_consumer/1. DevDelete task pending or running. Must to wait until it is finished"
          )

          %{response: {:error, :internal_server_error}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "start_consumer/1 failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition.id)
    end
  end

  defp stop_consumer(event_definition) do
    try do
      {:ok, consumer_state} = get_consumer_state(event_definition.id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
          handle_unknown_status(event_definition.id)

        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] or
            consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
          case EventPipeline.event_pipeline_running?(event_definition.id) do
            true ->
              ConsumerGroupSupervisor.stop_child(event_definition.id)

              consumer_status =
                case EventPipeline.event_pipeline_finished_processing?(event_definition.id) do
                  true ->
                    ConsumerStatusTypeEnum.status()[:paused_and_finished]

                  false ->
                    ConsumerStatusTypeEnum.status()[:paused_and_processing]
                end

              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: consumer_status,
                prev_status: consumer_state.status
              )

              %{response: {:ok, consumer_status}}

            false ->
              %{response: {:ok, consumer_state.status}}
          end

        consumer_state.status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
          consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
          case EventPipeline.event_pipeline_running?(event_definition.id) do
            true ->
              ConsumerGroupSupervisor.stop_child(event_definition.id)

              consumer_status =
                case EventPipeline.event_pipeline_finished_processing?(event_definition.id) do
                  true ->
                    ConsumerStatusTypeEnum.status()[:paused_and_finished]

                  false ->
                    ConsumerStatusTypeEnum.status()[:paused_and_processing]
                end

              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: consumer_state.status,
                prev_status: consumer_status
              )

              %{response: {:ok, consumer_status}}

            false ->
              %{response: {:ok, consumer_state.status}}
          end

        true ->
          ConsumerGroupSupervisor.stop_child(event_definition.id)

          consumer_status =
            case EventPipeline.event_pipeline_finished_processing?(event_definition.id) do
              true ->
                ConsumerStatusTypeEnum.status()[:paused_and_finished]

              false ->
                ConsumerStatusTypeEnum.status()[:paused_and_processing]
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_status,
            prev_status: consumer_state.status
          )

          %{response: {:ok, consumer_status}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "stop_consumer/1 failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition.id)
    end
  end

  defp backfill_notifications(notification_setting_id) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition_id = notification_setting.event_definition_id

    try do
      case EventDefinitionTaskMonitor.event_definition_task_running?(event_definition_id) do
        false ->
          {:ok, consumer_state} = get_consumer_state(event_definition_id)

          cond do
            consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
              handle_unknown_status(event_definition_id)

            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
              consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
                consumer_state.status ==
                  ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, BackfillNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  nil
              end

              %{
                response: {:ok, consumer_state.status}
              }

            true ->
              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: consumer_state.status
              )

              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, BackfillNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )
              end

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
              }
          end

        true ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to run backfill_notifications/1. DevDelete task pending or running. Must to wait until it is finished"
          )

          %{response: {:error, :internal_server_error}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "BackfillNotifications failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition_id)
    end
  end

  defp update_notifications(notification_setting_id) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition_id = notification_setting.event_definition_id

    try do
      case EventDefinitionTaskMonitor.event_definition_task_running?(event_definition_id) do
        false ->
          {:ok, consumer_state} = get_consumer_state(event_definition_id)

          cond do
            consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
              handle_unknown_status(event_definition_id)

            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
              consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
                consumer_state.status ==
                  ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, UpdateNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  nil
              end

              %{
                response: {:ok, consumer_state.status}
              }

            true ->
              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:update_notification_task_running],
                prev_status: consumer_state.status
              )

              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, UpdateNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )
              end

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
              }
          end

        true ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to run update_notifications/1. DevDelete task pending or running. Must to wait until it is finished"
          )

          %{response: {:error, :internal_server_error}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "UpdateNotifications failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition_id)
    end
  end

  defp delete_notifications(notification_setting_id) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition_id = notification_setting.event_definition_id

    try do
      case EventDefinitionTaskMonitor.event_definition_task_running?(event_definition_id) do
        false ->
          {:ok, consumer_state} = get_consumer_state(event_definition_id)

          cond do
            consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
              handle_unknown_status(event_definition_id)

            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
              consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
                consumer_state.status ==
                  ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, DeleteNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  nil
              end

              %{
                response: {:ok, consumer_state.status}
              }

            true ->
              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:delete_notification_task_running],
                prev_status: consumer_state.status
              )

              case create_job_queue_if_not_exists("notifications", event_definition_id) do
                {:ok, queue_name} ->
                  {:ok, _job_id} =
                    Exq.enqueue(Exq, queue_name, DeleteNotificationsWorker, [
                      notification_setting.id
                    ])

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )
              end

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:delete_notification_task_running]}
              }
          end

        true ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to run delete_notifications/1. DevDelete task pending or running. Must to wait until it is finished"
          )

          %{response: {:error, :internal_server_error}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "DeleteNotifications failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition_id)
    end
  end

  defp delete_events(event_definition_id) do
    case EventDefinitionTaskMonitor.event_definition_task_running?(event_definition_id) do
      false ->
        {:ok, consumer_state} = get_consumer_state(event_definition_id)

        cond do
          consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
            handle_unknown_status(event_definition_id)

          consumer_state.status == ConsumerStatusTypeEnum.status()[:running] ->
            %{response: {:error, consumer_state.status}}

          consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
            %{response: {:error, consumer_state.status}}

          consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
            %{response: {:error, consumer_state.status}}

          consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
            %{response: {:error, consumer_state.status}}

          true ->
            case create_job_queue_if_not_exists("events", event_definition_id) do
              {:ok, queue_name} ->
                {:ok, _job_id} =
                  Exq.enqueue(Exq, queue_name, DeleteEventDefinitionEventsWorker, [
                    event_definition_id
                  ])

              _ ->
                nil
            end

            %{response: {:ok, :success}}
        end

      true ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Failed to run delete_events/1. DevDelete task pending or running. Must to wait until it is finished"
        )

        %{response: {:error, :internal_server_error}}
    end
  end

  defp handle_unknown_status(event_definition_id) do
    event_definition = EventsContext.get_event_definition(event_definition_id)

    if event_definition.active == true do
      # update event_definition to be active false
      EventsContext.update_event_definition(event_definition, %{active: false, deleted_at: nil})
    end

    # check if there is a consumer running
    if EventPipeline.event_pipeline_running?(event_definition_id) do
      # Stop Consumer
      ConsumerGroupSupervisor.stop_child(event_definition_id)
    end

    # remove the ConsumerStatus Redis key
    Redis.key_delete("cs:#{event_definition_id}")

    # set the consumer status
    upsert_consumer_state(event_definition_id,
      status: ConsumerStatusTypeEnum.status()[:paused_and_finished],
      prev_status: ConsumerStatusTypeEnum.status()[:paused_and_finished],
      topic: event_definition.topic
    )

    %{response: {:ok, ConsumerStatusTypeEnum.status()[:paused_and_finished]}}
  end

  defp internal_error_state(event_definition_id) do
    {:ok, consumer_state} = get_consumer_state(event_definition_id)

    cond do
      consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
        handle_unknown_status(event_definition_id)
        %{response: {:error, :internal_server_error}}

      true ->
        upsert_consumer_state(
          event_definition_id,
          topic: consumer_state.topic,
          status: consumer_state.status,
          prev_status: consumer_state.prev_status
        )

        %{response: {:error, :internal_server_error}}
    end
  end

  defp create_job_queue_if_not_exists(queue_prefix, id) do
    case Exq.Api.queues(Exq.Api) do
      {:ok, queues} ->
        queue_name = queue_prefix <> "-" <> "#{id}"

        if !Enum.member?(queues, queue_name) do
          :ok = Exq.subscribe(Exq, queue_name, 5)
          CogyntLogger.info("#{__MODULE__}", "Created Queue: #{queue_name}")
        end

        {:ok, queue_name}

      _ ->
        CogyntLogger.error("#{__MODULE__}", "Exq.Api.queues/1 failed to fetch queues")
        {:error, :failed_to_fetch_queues}
    end
  end
end
