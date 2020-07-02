defmodule CogyntWorkstationIngest.Utils.ConsumerStateManager do
  @moduledoc """
  Genserver that keeps track of the State of each Consumer. Also knows which actions are
  allowed to be performed based on what state the consumer is in.
  """

  alias CogyntWorkstationIngest.Supervisors.{ConsumerGroupSupervisor, TaskSupervisor}
  alias CogyntWorkstationIngest.Servers.{ConsumerMonitor}
  alias CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  @default_state %{topic: nil, nsid: [], status: nil, prev_status: nil}

  def upsert_consumer_state(event_definition_id, opts) do
    status = Keyword.get(opts, :status, nil)
    topic = Keyword.get(opts, :topic, nil)
    prev_status = Keyword.get(opts, :prev_status, nil)
    nsid = Keyword.get(opts, :nsid, nil)
    module = Keyword.get(opts, :module, __MODULE__)

    IO.inspect(module, pretty: true, label: "@@@ Called From")

    case Redis.key_exists?("c:#{event_definition_id}") do
      {:ok, false} ->
        # create consumer state record
        consumer_state = %{
          status: status,
          topic: topic,
          prev_status: prev_status,
          nsid: nsid
        }

        Redis.hash_set_async("c:#{event_definition_id}", "consumer_state", consumer_state)

        CogyntLogger.info(
          "#{__MODULE__}",
          "New Consumer State Created for event_definition_id: #{event_definition_id}, #{
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

        consumer_state =
          if !is_nil(nsid) do
            Map.put(consumer_state, :nsid, nsid)
          else
            consumer_state
          end

        Redis.hash_set_async("c:#{event_definition_id}", "consumer_state", consumer_state)

        CogyntLogger.info(
          "#{__MODULE__}",
          "Consumer State Updated for event_definition_id: #{event_definition_id}, #{
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

  def get_consumer_state(event_definition_id) do
    case Redis.hash_get("c:#{event_definition_id}", "consumer_state", decode: true) do
      {:ok, nil} ->
        {:ok, @default_state}

      {:ok, consumer_state} ->
        {:ok, consumer_state}

      {:error, _} ->
        {:error, @default_state}
    end
  end

  def remove_consumer_state(event_definition_id) do
    for x <- ["a", "b", "c"], do: Redis.key_delete("#{x}:#{event_definition_id}")
  end

  # TODO
  def list_consumer_states() do
    # SCAN 0 MATCH "c:*"
    # Enum each result
    # HGETALL for each result
    # Format results to maps
    nil
  end

  def finished_processing?(event_definition_id) do
    case Redis.key_exists?("b:#{event_definition_id}") do
      {:ok, false} ->
        {:error, :key_does_not_exist}

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("b:#{event_definition_id}", "tmc")
        {:ok, tmp} = Redis.hash_get("b:#{event_definition_id}", "tmp")

        {:ok, String.to_integer(tmp) >= String.to_integer(tmc)}
    end
  end

  def manage_request(args) do
    %{response: _response} =
      Enum.reduce(args, %{}, fn
        {:start_consumer, event_definition}, _acc ->
          start_consumer(event_definition)

        {:stop_consumer, topic}, _acc ->
          stop_consumer(topic)

        {:backfill_notifications, notification_setting_id}, _acc ->
          backfill_notifications(notification_setting_id)

        {:update_notification_setting, notification_setting_id}, _acc ->
          update_notification_setting(notification_setting_id)

        {:delete_event_definition_events, event_definition_id}, _acc ->
          delete_events(event_definition_id)
      end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp start_consumer(event_definition) do
    try do
      {:ok, consumer_state} = get_consumer_state(event_definition.id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:running] ->
          %{response: {:ok, consumer_state.status}}

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_state.status,
            prev_status: ConsumerStatusTypeEnum.status()[:running],
            nsid: consumer_state.nsid
          )

          %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_state.status,
            prev_status: ConsumerStatusTypeEnum.status()[:running],
            nsid: consumer_state.nsid
          )

          %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}

        consumer_state.status == ConsumerStatusTypeEnum.status()[:topic_does_not_exist] ->
          %{response: {:ok, consumer_state.status}}

        true ->
          case ConsumerGroupSupervisor.start_child(event_definition) do
            {:error, nil} ->
              ConsumerRetryCache.retry_consumer(event_definition)

              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid
              )

              %{response: {:ok, ConsumerStatusTypeEnum.status()[:topic_does_not_exist]}}

            {:error, {:already_started, _pid}} ->
              upsert_consumer_state(
                event_definition.id,
                topic: event_definition.topic,
                status: ConsumerStatusTypeEnum.status()[:running],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid
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
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid
              )

              %{response: {:ok, ConsumerStatusTypeEnum.status()[:running]}}
          end
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

  defp stop_consumer(topic) do
    event_definition = EventsContext.get_event_definition_by(%{topic: topic})

    try do
      {:ok, consumer_state} = get_consumer_state(event_definition.id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
          %{response: {:ok, consumer_state.status}}

        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
          %{response: {:ok, consumer_state.status}}

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
          consumer_status =
            case finished_processing?(event_definition.id) do
              {:ok, true} ->
                ConsumerStatusTypeEnum.status()[:paused_and_finished]

              {:ok, false} ->
                ConsumerStatusTypeEnum.status()[:paused_and_processing]
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_state.status,
            prev_status: consumer_status,
            nsid: consumer_state.nsid
          )

          %{response: {:ok, consumer_state.status}}

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
          consumer_status =
            case finished_processing?(event_definition.id) do
              {:ok, true} ->
                ConsumerStatusTypeEnum.status()[:paused_and_finished]

              {:ok, false} ->
                ConsumerStatusTypeEnum.status()[:paused_and_processing]
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_state.status,
            prev_status: consumer_status,
            nsid: consumer_state.nsid
          )

          %{response: {:ok, consumer_state.status}}

        true ->
          ConsumerGroupSupervisor.stop_child(topic)

          consumer_status =
            case finished_processing?(event_definition.id) do
              {:ok, true} ->
                ConsumerStatusTypeEnum.status()[:paused_and_finished]

              {:ok, false} ->
                ConsumerStatusTypeEnum.status()[:paused_and_processing]
            end

          upsert_consumer_state(
            event_definition.id,
            topic: event_definition.topic,
            status: consumer_status,
            prev_status: consumer_state.status,
            nsid: consumer_state.nsid
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
      {:ok, consumer_state} = get_consumer_state(event_definition_id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
          TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: consumer_state.status,
            nsid: consumer_state.nsid ++ [notification_setting_id]
          )

          %{
            response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
          }

        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: consumer_state.status,
            nsid: consumer_state.nsid ++ [notification_setting_id]
          )

          %{
            response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
          }

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
          if Enum.member?(consumer_state.nsid, notification_setting_id) do
            TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})
          end

          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: consumer_state.status,
            prev_status: consumer_state.prev_status,
            nsid: Enum.uniq(consumer_state.nsid ++ [notification_setting_id])
          )

          %{response: {:ok, consumer_state.status}}

        true ->
          ConsumerGroupSupervisor.stop_child(consumer_state.topic)

          case finished_processing?(event_definition_id) do
            {:ok, true} ->
              TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid ++ [notification_setting_id]
              )

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
              }

            {:ok, false} ->
              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid ++ [notification_setting_id]
              )

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
              }
          end
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "backfill_notifications failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition_id)
    end
  end

  defp update_notification_setting(notification_setting_id) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition_id = notification_setting.event_definition_id

    try do
      {:ok, consumer_state} = get_consumer_state(event_definition_id)

      cond do
        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
          TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})

          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: ConsumerStatusTypeEnum.status()[:update_notification_task_running],
            prev_status: consumer_state.status,
            nsid: consumer_state.nsid ++ [notification_setting_id]
          )

          %{
            response: {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
          }

        consumer_state.status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: ConsumerStatusTypeEnum.status()[:update_notification_task_running],
            prev_status: consumer_state.status,
            nsid: consumer_state.nsid ++ [notification_setting_id]
          )

          %{
            response: {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
          }

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
          if Enum.member?(consumer_state.nsid, notification_setting_id) do
            TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})
          end

          upsert_consumer_state(
            event_definition_id,
            topic: consumer_state.topic,
            status: consumer_state.status,
            prev_status: consumer_state.prev_status,
            nsid: Enum.uniq(consumer_state.nsid ++ [notification_setting_id])
          )

          %{response: {:ok, consumer_state.status}}

        true ->
          ConsumerGroupSupervisor.stop_child(consumer_state.topic)

          case finished_processing?(event_definition_id) do
            {:ok, true} ->
              TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})

              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:update_notification_task_running],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid ++ [notification_setting_id]
              )

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
              }

            {:ok, false} ->
              upsert_consumer_state(
                event_definition_id,
                topic: consumer_state.topic,
                status: ConsumerStatusTypeEnum.status()[:update_notification_task_running],
                prev_status: consumer_state.status,
                nsid: consumer_state.nsid ++ [notification_setting_id]
              )

              %{
                response:
                  {:ok, ConsumerStatusTypeEnum.status()[:update_notification_task_running]}
              }
          end
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "update_notification_setting failed with error: #{inspect(error, pretty: true)}"
        )

        internal_error_state(event_definition_id)
    end
  end

  defp delete_events(event_definition_id) do
    {:ok, consumer_state} = get_consumer_state(event_definition_id)

    cond do
      consumer_state.status == ConsumerStatusTypeEnum.status()[:running] ->
        %{response: {:error, consumer_state.status}}

      consumer_state.status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        %{response: {:error, consumer_state.status}}

      consumer_state.status ==
          ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
        %{response: {:error, consumer_state.status}}

      true ->
        TaskSupervisor.start_child(%{delete_event_definition_events: event_definition_id})
        %{response: {:ok, :success}}
    end
  end

  defp internal_error_state(event_definition_id) do
    {:ok, consumer_state} = get_consumer_state(event_definition_id)

    case consumer_state.status do
      nil ->
        consumer_status =
          case finished_processing?(event_definition_id) do
            {:ok, true} ->
              ConsumerStatusTypeEnum.status()[:paused_and_finished]

            {:ok, false} ->
              ConsumerStatusTypeEnum.status()[:paused_and_processing]
          end

        upsert_consumer_state(
          event_definition_id,
          topic: consumer_state.topic,
          status: consumer_status,
          prev_status: consumer_state.prev_status,
          nsid: []
        )

        %{response: {:error, :internal_server_error}}

      _ ->
        upsert_consumer_state(
          event_definition_id,
          topic: consumer_state.topic,
          status: consumer_state.status,
          prev_status: consumer_state.prev_status,
          nsid: []
        )

        %{response: {:error, :internal_server_error}}
    end
  end
end
