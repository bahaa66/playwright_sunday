defmodule CogyntWorkstationIngest.Servers.Caches.NotificationSubscriptionCache do
  @moduledoc """
  This cache is used to trigger notification subscriptions in batches via Redis pub/sub.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config

  @notifications_key "_notifications"

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(
      __MODULE__,
      [
        {:ets_table_name, :notification_subscription_cache},
        {:log_limit, 500_000}
      ],
      name: __MODULE__
    )
  end

  def add_notifications(notifications) when is_list(notifications) do
    GenServer.cast(__MODULE__, {:add_notifications, notifications})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(args) do
    [{:ets_table_name, table_name}, {:log_limit, log_limit}] = args
    :ets.new(table_name, [:named_table, :set])

    timer_ref = Process.send_after(__MODULE__, :tick, Config.notification_subscription_timer())

    {:ok,
     %{
       timer: timer_ref,
       log_limit: log_limit,
       ets_table_name: table_name
     }}
  end

  @impl true
  def handle_cast(
        {:add_notifications, notifications},
        %{ets_table_name: table_name} = state
      ) do
    case :ets.lookup(table_name, @notifications_key) do
      [] ->
        true = :ets.insert(table_name, {@notifications_key, notifications})

      [{key, existing_notifications}] ->
        true = :ets.insert(table_name, {key, existing_notifications ++ notifications})
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %{ets_table_name: table_name} = state) do
    timer_ref = Process.send_after(__MODULE__, :tick, Config.notification_subscription_timer())

    case :ets.lookup(table_name, @notifications_key) do
      [] ->
        {:noreply, %{state | timer: timer_ref}}

      [{key, notifications}] ->
        true = :ets.delete(table_name, key)

        if is_null_or_empty?(notifications) == false do
          Redis.publish_async("notifications_subscription", %{notifications: notifications})
        end

        {:noreply, %{state | timer: timer_ref}}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp is_null_or_empty?(enumerable) do
    is_nil(enumerable) or Enum.empty?(enumerable)
  end
end
