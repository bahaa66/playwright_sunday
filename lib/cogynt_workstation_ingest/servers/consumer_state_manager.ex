defmodule CogyntWorkstationIngest.Servers.ConsumerStateManager do
  @moduledoc """

  """

  use GenServer
  alias CogyntWorkstationIngest.Supervisors.{ConsumerGroupSupervisor, TaskSupervisor}
  alias CogyntWorkstationIngest.Servers.{ConsumerMonitor, NotificationsTaskMonitor}
  alias CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Broadway.Producer

  @default_state %{topic: nil, nsid: [], status: nil, prev_status: nil}

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def update_consumer_state(event_definition_id, topic, status, module_name, nsid \\ []) do
    CogyntLogger.warn(
      "#{__MODULE__}",
      "Adding Consumer State: ID: #{event_definition_id}, status: #{status}, Module: #{
        module_name
      }"
    )

    GenServer.cast(__MODULE__, {:update_consumer_state, event_definition_id, topic, status, nsid})
  end

  def get_consumer_state(event_definition_id) do
    GenServer.call(__MODULE__, {:get_consumer_state, event_definition_id})
  end

  def remove_consumer_state(event_definition_id) do
    GenServer.cast(__MODULE__, {:remove_consumer_state, event_definition_id})
  end

  def list_consumer_states() do
    GenServer.call(__MODULE__, {:list_consumer_states})
  end

  def manage_request(args) do
    GenServer.call(__MODULE__, {:manage_request, args})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast(
        {:update_consumer_state, event_definition_id, topic, status, nsid},
        state
      ) do
    %{status: current_status} = Map.get(state, event_definition_id, @default_state)

    new_state =
      Map.put(state, event_definition_id, %{
        topic: topic,
        nsid: nsid,
        status: status,
        prev_status: current_status
      })

    {:noreply, new_state}
  end

  @impl true
  def handle_call({:get_consumer_state, event_definition_id}, _from, state) do
    {:reply, Map.get(state, event_definition_id, @default_state), state}
  end

  @impl true
  def handle_cast({:remove_consumer_state, event_definition_id}, state) do
    new_state = Map.delete(state, event_definition_id)
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:list_consumer_states}, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:manage_request, args}, _from, state) do
    %{state: new_state, response: response} =
      Enum.reduce(args, %{}, fn
        {:start_consumer, event_definition}, _acc ->
          start_consumer(event_definition, state)

        {:stop_consumer, topic}, _acc ->
          stop_consumer(topic, state)

        {:backfill_notifications, notification_setting_id}, _acc ->
          backfill_notifications(notification_setting_id, state)

        {:update_notification_setting, notification_setting_id}, _acc ->
          update_notification_setting(notification_setting_id, state)

        {:delete_event_definition_events, event_definition_id}, _acc ->
          delete_events(event_definition_id, state)
      end)

    CogyntLogger.info("#{__MODULE__}", "Consumer State Manager New State: #{inspect(new_state)}")

    {:reply, response, new_state}
  end

  defp start_consumer(event_definition, state) do
    %{status: status, nsid: nsid} = Map.get(state, event_definition.id, @default_state)

    cond do
      status == ConsumerStatusTypeEnum.status()[:running] ->
        %{state: state, response: {:error, status}}

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            nsid: nsid,
            status: status,
            prev_status: ConsumerStatusTypeEnum.status()[:running]
          })

        %{
          state: new_state,
          response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
        }

      status == ConsumerStatusTypeEnum.status()[:topic_does_not_exist] ->
        %{
          state: state,
          response: {:error, status}
        }

      true ->
        case ConsumerGroupSupervisor.start_child(event_definition) do
          {:ok, nil} ->
            ConsumerRetryCache.retry_consumer(event_definition)

            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                nsid: nsid,
                status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist],
                prev_status: status
              })

            %{
              state: new_state,
              response: {:error, ConsumerStatusTypeEnum.status()[:topic_does_not_exist]}
            }

          {:ok, pid} ->
            ConsumerMonitor.monitor(
              pid,
              event_definition.id,
              event_definition.topic,
              event_definition.event_type
            )

            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                nsid: nsid,
                status: ConsumerStatusTypeEnum.status()[:running],
                prev_status: status
              })

            %{state: new_state, response: {:ok, pid}}
        end
    end
  end

  defp stop_consumer(topic, state) do
    event_definition = EventsContext.get_event_definition_by(%{topic: topic})

    %{status: status, nsid: nsid} = Map.get(state, event_definition.id, @default_state)

    cond do
      status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
        %{state: state, response: {:ok, status}}

      status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
        %{state: state, response: {:ok, status}}

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            nsid: nsid,
            status: status,
            prev_status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
          })

        %{
          state: new_state,
          response: {:ok, status}
        }

      true ->
        ConsumerGroupSupervisor.stop_child(topic)

        consumer_status =
          case Producer.is_processing?(event_definition.id, event_definition.event_type) do
            true ->
              ConsumerStatusTypeEnum.status()[:paused_and_processing]

            false ->
              ConsumerStatusTypeEnum.status()[:paused_and_finished]
          end

        new_state =
          Map.put(state, event_definition.id, %{
            topic: topic,
            nsid: nsid,
            status: consumer_status,
            prev_status: status
          })

        %{state: new_state, response: {:ok, consumer_status}}
    end
  end

  defp backfill_notifications(notification_setting_id, state) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition =
      EventsContext.get_event_definition(notification_setting.event_definition_id)

    %{status: status, prev_status: prev_status, nsid: nsid} =
      Map.get(state, event_definition.id, @default_state)

    cond do
      status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
        TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            nsid: nsid ++ [notification_setting_id],
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: status
          })

        %{
          state: new_state,
          response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
        }

      status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            nsid: nsid ++ [notification_setting_id],
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: status
          })

        %{
          state: new_state,
          response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
        }

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        case NotificationsTaskMonitor.is_processing?(notification_setting_id) do
          true ->
            %{
              state: state,
              response: {:ok, status}
            }

          false ->
            TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                nsid: Enum.uniq(nsid ++ [notification_setting_id]),
                status: status,
                prev_status: prev_status
              })

            %{
              state: new_state,
              response: {:ok, status}
            }
        end

      true ->
        ConsumerGroupSupervisor.stop_child(event_definition.topic)

        case Producer.is_processing?(event_definition.id, event_definition.event_type) do
          true ->
            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                nsid: nsid ++ [notification_setting_id],
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: status
              })

            %{
              state: new_state,
              response:
                {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
            }

          false ->
            TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                nsid: nsid ++ [notification_setting_id],
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: status
              })

            %{
              state: new_state,
              response:
                {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
            }
        end
    end
  end

  defp update_notification_setting(notification_setting_id, state) do
    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    event_definition =
      EventsContext.get_event_definition(notification_setting.event_definition_id)

    %{status: status} =
      Map.get(state, event_definition.id, %{topic: nil, status: nil, prev_status: nil})

    cond do
      status == ConsumerStatusTypeEnum.status()[:paused_and_finished] ->
        TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})

        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: status
          })

        %{
          state: new_state,
          response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
        }

      status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
        new_state =
          Map.put(state, event_definition.id, %{
            topic: event_definition.topic,
            status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
            prev_status: status
          })

        %{
          state: new_state,
          response: {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
        }

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        %{
          state: state,
          response: {:ok, status}
        }

      true ->
        ConsumerGroupSupervisor.stop_child(event_definition.topic)

        case Producer.is_processing?(event_definition.id, event_definition.event_type) do
          true ->
            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: status
              })

            %{
              state: new_state,
              response:
                {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
            }

          false ->
            TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})

            new_state =
              Map.put(state, event_definition.id, %{
                topic: event_definition.topic,
                status: ConsumerStatusTypeEnum.status()[:backfill_notification_task_running],
                prev_status: status
              })

            %{
              state: new_state,
              response:
                {:ok, ConsumerStatusTypeEnum.status()[:backfill_notification_task_running]}
            }
        end
    end
  end

  defp delete_events(event_definition_id, state) do
    %{status: status} =
      Map.get(state, event_definition_id, %{topic: nil, status: nil, prev_status: nil})

    cond do
      status == ConsumerStatusTypeEnum.status()[:running] ->
        %{state: state, response: {:error, status}}

      status == ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        %{state: state, response: {:error, status}}

      true ->
        TaskSupervisor.start_child(%{delete_event_definition_events: event_definition_id})
        %{state: state, response: {:ok, :success}}
    end
  end
end
