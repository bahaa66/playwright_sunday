defmodule CogyntWorkstationIngest.Servers.NotificationsTaskMonitor do
  @moduledoc """
    Module that monitors the the status of Notification Tasks.
    Will push the status to Cogynt-OTP via RPC call when the Tasks
    are created and shutdown
  """

  use GenServer
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid, type, notification_setting_id) do
    GenServer.cast(__MODULE__, {:monitor, pid, type, notification_setting_id})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid, :backfill, notification_setting_id}, state) do
    Process.monitor(pid)

    new_state = Map.put(state, pid, %{id: notification_setting_id, type: :backfill})

    case Redis.hash_get("ts", "bn") do
      {:ok, nil} ->
        Redis.hash_set(
          "ts",
          "bn",
          [notification_setting_id]
        )

      {:ok, notification_setting_ids} ->
        Redis.hash_set(
          "ts",
          "bn",
          Enum.uniq(notification_setting_ids ++ [notification_setting_id])
        )
    end

    Redis.key_pexpire("ts", 60000)

    Redis.publish_async("notification_settings_subscription", %{
      id: notification_setting_id,
      status: "running"
    })

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:monitor, pid, :update, notification_setting_id}, state) do
    Process.monitor(pid)

    new_state = Map.put(state, pid, %{id: notification_setting_id, type: :update})

    case Redis.hash_get("ts", "un") do
      {:ok, nil} ->
        Redis.hash_set(
          "ts",
          "un",
          [notification_setting_id]
        )

      {:ok, notification_setting_ids} ->
        Redis.hash_set(
          "ts",
          "un",
          Enum.uniq(notification_setting_ids ++ [notification_setting_id])
        )
    end

    Redis.key_pexpire("ts", 60000)

    Redis.publish_async("notification_settings_subscription", %{
      id: notification_setting_id,
      status: "running"
    })

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:monitor, pid, :delete, notification_setting_id}, state) do
    Process.monitor(pid)

    new_state = Map.put(state, pid, %{id: notification_setting_id, type: :delete})

    case Redis.hash_get("ts", "dn") do
      {:ok, nil} ->
        Redis.hash_set(
          "ts",
          "dn",
          [notification_setting_id]
        )

      {:ok, notification_setting_ids} ->
        Redis.hash_set(
          "ts",
          "dn",
          Enum.uniq(notification_setting_ids ++ [notification_setting_id])
        )
    end

    Redis.key_pexpire("ts", 60000)

    Redis.publish_async("notification_settings_subscription", %{
      id: notification_setting_id,
      status: "running"
    })

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    # TODO implement retry for backfill/update task if reason anything other than :normal or :shutdown

    %{id: notification_setting_id, type: type} = Map.get(state, pid)

    notification_setting = NotificationsContext.get_notification_setting(notification_setting_id)

    {:ok,
     %{
       backfill_notifications: backfill_notifications,
       update_notifications: update_notifications,
       delete_notifications: delete_notifications,
       prev_status: prev_status,
       topic: topic
     }} = ConsumerStateManager.get_consumer_state(notification_setting.event_definition_id)

    case type do
      :backfill ->
        backfill_notifications = List.delete(backfill_notifications, notification_setting_id)

        if Enum.empty?(backfill_notifications) do
          cond do
            prev_status == ConsumerStatusTypeEnum.status()[:running] ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                backfill_notifications: backfill_notifications,
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )

              event_definition_map =
                EventsContext.get_event_definition(notification_setting.event_definition_id)
                |> EventsContext.remove_event_definition_virtual_fields()

              ConsumerStateManager.manage_request(%{start_consumer: event_definition_map})

            true ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                backfill_notifications: backfill_notifications,
                prev_status: ConsumerStatusTypeEnum.status()[:paused_and_processing],
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )
          end
        else
          ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
            backfill_notifications: backfill_notifications
          )

          Enum.each(backfill_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Triggering backfill notifications task: #{inspect(notification_setting_id)}"
            )

            ConsumerStateManager.manage_request(%{
              backfill_notifications: notification_setting_id
            })
          end)
        end

        case Redis.hash_get("ts", "bn") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            notification_setting_ids =
              List.delete(notification_setting_ids, notification_setting_id)

            Redis.hash_set(
              "ts",
              "bn",
              notification_setting_ids
            )
        end

      :update ->
        update_notifications = List.delete(update_notifications, notification_setting_id)

        if Enum.empty?(update_notifications) do
          cond do
            prev_status == ConsumerStatusTypeEnum.status()[:running] ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                update_notifications: update_notifications,
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )

              event_definition_map =
                EventsContext.get_event_definition(notification_setting.event_definition_id)
                |> EventsContext.remove_event_definition_virtual_fields()

              ConsumerStateManager.manage_request(%{start_consumer: event_definition_map})

            true ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                update_notifications: update_notifications,
                prev_status: ConsumerStatusTypeEnum.status()[:paused_and_processing],
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )
          end
        else
          ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
            update_notifications: update_notifications
          )

          Enum.each(update_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Triggering update notifications task: #{inspect(notification_setting_id)}"
            )

            ConsumerStateManager.manage_request(%{
              update_notifications: notification_setting_id
            })
          end)
        end

        case Redis.hash_get("ts", "un") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            notification_setting_ids =
              List.delete(notification_setting_ids, notification_setting_id)

            Redis.hash_set(
              "ts",
              "un",
              notification_setting_ids
            )
        end

      :delete ->
        delete_notifications = List.delete(delete_notifications, notification_setting_id)

        if Enum.empty?(delete_notifications) do
          cond do
            prev_status == ConsumerStatusTypeEnum.status()[:running] ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                delete_notifications: delete_notifications,
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )

              event_definition_map =
                EventsContext.get_event_definition(notification_setting.event_definition_id)
                |> EventsContext.remove_event_definition_virtual_fields()

              ConsumerStateManager.manage_request(%{start_consumer: event_definition_map})

            true ->
              ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
                topic: topic,
                delete_notifications: delete_notifications,
                prev_status: ConsumerStatusTypeEnum.status()[:paused_and_processing],
                status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
              )
          end
        else
          ConsumerStateManager.upsert_consumer_state(notification_setting.event_definition_id,
            delete_notifications: delete_notifications
          )

          Enum.each(delete_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Triggering delete notifications task: #{inspect(notification_setting_id)}"
            )

            ConsumerStateManager.manage_request(%{
              delete_notifications: notification_setting_id
            })
          end)
        end

        case Redis.hash_get("ts", "dn") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            notification_setting_ids =
              List.delete(notification_setting_ids, notification_setting_id)

            Redis.hash_set(
              "ts",
              "dn",
              notification_setting_ids
            )
        end
    end

    Redis.key_pexpire("ts", 60000)

    Redis.publish_async("notification_settings_subscription", %{
      id: notification_setting_id,
      status: "finished"
    })

    new_state = Map.delete(state, pid)
    {:noreply, new_state}
  end
end
