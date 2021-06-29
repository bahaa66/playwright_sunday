defmodule CogyntWorkstationIngest.Utils.ConsumerStateManager do
  @moduledoc """
  Genserver that keeps track of the State of each Consumer. Acts as a State Machine
  that will move ConsumerStatus from one state to another based on the current consumer status
  or previous consumer status at the time of the request
  """
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.ConsumerMonitor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    BackfillNotificationsWorker,
    UpdateNotificationsWorker,
    DeleteNotificationsWorker
  }

  alias CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers

  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Events.EventDefinition

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

    consumer_state =
      case Redis.hash_get("cs", event_definition_id) do
        {:ok, consumer_state} ->
          consumer_state || @default_state

        {:error, error} ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "Error trying to determine consumer state from Redis for EventDefinitionId: #{
              event_definition_id
            }, #{inspect(error)}"
          )

          @default_state
      end

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

    Redis.hash_set_async("cs", event_definition_id, consumer_state)

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

  @doc """
  fetches the consumer_state map stored in the Redis hashkey cs:
  """
  def get_consumer_state(event_definition_id) do
    case Redis.hash_get("cs", event_definition_id) do
      {:ok, consumer_state} ->
        {:ok, consumer_state || @default_state}

      {:error, _} ->
        {:error, @default_state}
    end
  end

  @doc """
  removes all redis keys that are associated with the given event_definition_id
  """
  def remove_consumer_state(event_definition_id, _opts \\ []) do
    for x <- ["fem", "emi"], do: Redis.key_delete("#{x}:#{event_definition_id}")

    Redis.hash_delete("cs", event_definition_id)
    Redis.hash_delete("ecgid", "EventDefinition-#{event_definition_id}")

    case Redis.get("dd") do
      {:ok, values} when is_list(values) ->
        if Enum.member?(values, event_definition_id) do
          values = List.delete(values, event_definition_id)

          if Enum.empty?(values) do
            Redis.key_delete("dd")
          else
            Redis.set("dd", Enum.uniq(values))
            # 7 mins
            Redis.key_pexpire("dd", 420_000)
          end
        end

      _ ->
        nil
    end

    Redis.hash_delete("crw", event_definition_id)
    # Reset JobQs
    ExqHelpers.unubscribe_and_remove("events-#{event_definition_id}")
    ExqHelpers.unubscribe_and_remove("notifications-#{event_definition_id}")
  end

  def manage_request(args) do
    %{response: _response} =
      Enum.reduce(args, %{}, fn
        {:start_consumer, event_definition}, _acc ->
          start_consumer(event_definition)

        {:stop_consumer, event_definition}, _acc ->
          stop_consumer(event_definition)

        {:stop_consumer_for_notification_tasks, event_definition}, _acc ->
          stop_consumer_for_notification_tasks(event_definition)

        {:shutdown_consumer, event_definition}, _acc ->
          shutdown_consumer(event_definition)

        {:backfill_notifications, notification_setting_id}, _acc ->
          backfill_notifications(notification_setting_id)

        {:update_notifications, notification_setting_id}, _acc ->
          update_notifications(notification_setting_id)

        {:delete_notifications, notification_setting_id}, _acc ->
          delete_notifications(notification_setting_id)

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
      case event_definition_task_running?(event_definition.id) do
        false ->
          {:ok, consumer_state} = get_consumer_state(event_definition.id)

          cond do
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

            true ->
              # Ensure that it is indeed started and running on the pod that recieved the pub_sub
              case EventPipeline.pipeline_started?(event_definition.id) do
                true ->
                  case EventPipeline.pipeline_running?(event_definition.id) do
                    true ->
                      nil

                    false ->
                      EventPipeline.resume_pipeline(event_definition.id)
                  end

                  if consumer_state.status !=
                       ConsumerStatusTypeEnum.status()[:running] do
                    upsert_consumer_state(
                      event_definition.id,
                      topic: event_definition.topic,
                      status: ConsumerStatusTypeEnum.status()[:running],
                      prev_status: consumer_state.status
                    )

                    %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}
                  else
                    %{response: {:ok, consumer_state.status}}
                  end

                false ->
                  {:ok, current_consumer_status} = start_pipeline(event_definition)

                  upsert_consumer_state(
                    event_definition.id,
                    topic: event_definition.topic,
                    status: current_consumer_status,
                    prev_status: consumer_state.status
                  )

                  %{response: {:ok, current_consumer_status}}
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

        consumer_state.status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
          consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
          case EventPipeline.pipeline_running?(event_definition.id) do
            true ->
              {:ok, current_consumer_status} = suspend_pipeline(event_definition)

              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: consumer_state.status,
                prev_status: current_consumer_status
              )

              %{response: {:ok, current_consumer_status}}

            false ->
              %{response: {:ok, consumer_state.status}}
          end

        true ->
          {:ok, current_consumer_status} =
            case EventPipeline.pipeline_running?(event_definition.id) do
              true ->
                suspend_pipeline(event_definition)

              false ->
                case EventPipeline.pipeline_finished_processing?(event_definition.id) do
                  true ->
                    {:ok, ConsumerStatusTypeEnum.status()[:paused_and_finished]}

                  false ->
                    {:ok, ConsumerStatusTypeEnum.status()[:paused_and_processing]}
                end
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: current_consumer_status,
            prev_status: consumer_state.status
          )

          %{response: {:ok, current_consumer_status}}
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

  # This is specifically for pausing consumer for the JobQueue workers until a updated_at field
  # is implemented for the cs:* Redis hashfield values
  defp stop_consumer_for_notification_tasks(event_definition) do
    try do
      {:ok, consumer_state} = get_consumer_state(event_definition.id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
          handle_unknown_status(event_definition.id)

        consumer_state.status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] or
          consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] or
            consumer_state.status ==
              ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
          case EventPipeline.pipeline_running?(event_definition.id) do
            true ->
              {:ok, current_consumer_status} = suspend_pipeline(event_definition)
              %{response: {:ok, current_consumer_status}}

            false ->
              %{response: {:ok, consumer_state.status}}
          end

        true ->
          {:ok, current_consumer_status} =
            case EventPipeline.pipeline_running?(event_definition.id) do
              true ->
                suspend_pipeline(event_definition)

              false ->
                case EventPipeline.pipeline_finished_processing?(event_definition.id) do
                  true ->
                    {:ok, ConsumerStatusTypeEnum.status()[:paused_and_finished]}

                  false ->
                    {:ok, ConsumerStatusTypeEnum.status()[:paused_and_processing]}
                end
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: current_consumer_status,
            prev_status: consumer_state.status
          )

          %{response: {:ok, current_consumer_status}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "stop_consumer_for_notification_tasks/1 failed with error: #{
            inspect(error, pretty: true)
          }"
        )

        internal_error_state(event_definition.id)
    end
  end

  defp shutdown_consumer(event_definition) do
    try do
      {:ok, consumer_state} = get_consumer_state(event_definition.id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:unknown] ->
          handle_unknown_status(event_definition.id)

        true ->
          ConsumerGroupSupervisor.stop_child(event_definition.id)
          %{response: {:ok, ConsumerStatusTypeEnum.status()[:unknown]}}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "shutdown_consumer/1 failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition.id)
    end
  end

  defp backfill_notifications(notification_setting_id) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition_id = notification_setting.event_definition_id

    try do
      case event_definition_task_running?(event_definition_id) do
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
              ExqHelpers.create_and_enqueue(
                "notifications",
                event_definition_id,
                BackfillNotificationsWorker,
                notification_setting.id
              )

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

              case ExqHelpers.create_and_enqueue(
                     "notifications",
                     event_definition_id,
                     BackfillNotificationsWorker,
                     notification_setting.id
                   ) do
                {:ok, _} ->
                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
                  }

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )

                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
                  }
              end
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
      case event_definition_task_running?(event_definition_id) do
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
              ExqHelpers.create_and_enqueue(
                "notifications",
                event_definition_id,
                UpdateNotificationsWorker,
                notification_setting.id
              )

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

              case ExqHelpers.create_and_enqueue(
                     "notifications",
                     event_definition_id,
                     UpdateNotificationsWorker,
                     notification_setting.id
                   ) do
                {:ok, _} ->
                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
                  }

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )

                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
                  }
              end
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
      case event_definition_task_running?(event_definition_id) do
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
              ExqHelpers.create_and_enqueue(
                "notifications",
                event_definition_id,
                DeleteNotificationsWorker,
                notification_setting.id
              )

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

              case ExqHelpers.create_and_enqueue(
                     "notifications",
                     event_definition_id,
                     DeleteNotificationsWorker,
                     notification_setting.id
                   ) do
                {:ok, _} ->
                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:delete_notification_task_running]}
                  }

                _ ->
                  # Something failed when queueing the job. Reset the consumer_state
                  upsert_consumer_state(
                    event_definition_id,
                    topic: consumer_state.topic,
                    status: consumer_state.status,
                    prev_status: consumer_state.prev_status
                  )

                  %{
                    response:
                      {:ok, ConsumerStatusTypeEnum.status()[:delete_notification_task_running]}
                  }
              end
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

  # ---------------------- #
  # --- Helper Methods --- #
  # ---------------------- #
  defp start_pipeline(event_definition) do
    case ConsumerGroupSupervisor.start_child(event_definition) do
      {:error, nil} ->
        Redis.hash_set_async("crw", event_definition.id, "et")

        {:ok, ConsumerStatusTypeEnum.status()[:topic_does_not_exist]}

      {:error, {:already_started, _pid}} ->
        # subscribe/resubscribe to JobQs
        ExqHelpers.create_job_queue_if_not_exists("events", event_definition.id)
        ExqHelpers.create_job_queue_if_not_exists("notifications", event_definition.id)

        {:ok, ConsumerStatusTypeEnum.status()[:running]}

      {:ok, pid} ->
        ConsumerMonitor.monitor(
          pid,
          event_definition.id,
          event_definition.topic
        )

        # subscribe/resubscribe to JobQs
        ExqHelpers.create_job_queue_if_not_exists("events", event_definition.id)
        ExqHelpers.create_job_queue_if_not_exists("notifications", event_definition.id)

        {:ok, ConsumerStatusTypeEnum.status()[:running]}
    end
  end

  defp suspend_pipeline(event_definition) do
    EventPipeline.suspend_pipeline(event_definition.id)

    consumer_status =
      case EventPipeline.pipeline_finished_processing?(event_definition.id) do
        true ->
          ConsumerStatusTypeEnum.status()[:paused_and_finished]

        false ->
          ConsumerStatusTypeEnum.status()[:paused_and_processing]
      end

    {:ok, consumer_status}
  end

  defp handle_unknown_status(event_definition_id) do
    case EventsContext.get_event_definition(event_definition_id) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "handle_unknown_status/1 no event_definition found for id: #{event_definition_id}"
        )

        # check if there is a consumer running
        if EventPipeline.pipeline_started?(event_definition_id) do
          ConsumerGroupSupervisor.stop_child(event_definition_id)
        end

        remove_consumer_state(event_definition_id)

        %{response: {:ok, nil}}

      %EventDefinition{active: true} = event_definition ->
        # update event_definition to be active false
        EventsContext.update_event_definition(event_definition, %{active: false, deleted_at: nil})

        # check if there is a consumer running
        if EventPipeline.pipeline_started?(event_definition_id) do
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

      _ ->
        %{response: {:ok, nil}}
    end
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

  defp event_definition_task_running?(event_definition_id) do
    case Redis.get("dd") do
      {:ok, nil} ->
        case Redis.hash_get("ts", "de") do
          {:ok, nil} ->
            false

          {:ok, values} ->
            Enum.member?(values, event_definition_id)

          {:error, _} ->
            false

          _ ->
            false
        end

      {:ok, values} ->
        Enum.member?(values, event_definition_id)

      {:error, _} ->
        false

      _ ->
        false
    end
  end
end
