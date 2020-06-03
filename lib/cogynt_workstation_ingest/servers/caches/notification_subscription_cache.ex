defmodule CogyntWorkstationIngest.Servers.Caches.NotificationSubscriptionCache do
  @moduledoc """
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient

  @key_new "_new_notifications"
  @key_updated "_updated_notifications"

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

  def add_new_notifications(notifications) when is_list(notifications) do
    GenServer.cast(__MODULE__, {:add_new_notifications, notifications})
  end

  def add_updated_notifications(notifications) when is_list(notifications) do
    GenServer.cast(__MODULE__, {:add_updated_notifications, notifications})
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
        {:add_new_notifications, notifications},
        %{ets_table_name: table_name} = state
      ) do
    case :ets.lookup(table_name, @key_new) do
      [] ->
        true = :ets.insert(table_name, {@key_new, notifications})

      [{key, existing_notifications}] ->
        true = :ets.insert(table_name, {key, existing_notifications ++ notifications})
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:add_updated_notifications, notifications},
        %{ets_table_name: table_name} = state
      ) do
    case :ets.lookup(table_name, @key_updated) do
      [] ->
        true = :ets.insert(table_name, {@key_updated, notifications})

      [{key, existing_notifications}] ->
        true = :ets.insert(table_name, {key, existing_notifications ++ notifications})
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:tick, %{ets_table_name: table_name} = state) do
    timer_ref = Process.send_after(__MODULE__, :tick, Config.notification_subscription_timer())

    # Grab all the records in the ets table and put them into a list
    ets_records = :ets.tab2list(table_name)

    Enum.each(ets_records, fn {key, notifications} ->
      case key do
        @key_new ->
          true = :ets.delete(table_name, @key_new)
          CogyntClient.publish_notifications(notifications)

        @key_updated ->
          true = :ets.delete(table_name, @key_updated)
          CogyntClient.publish_updated_notifications(notifications)
      end
    end)

    {:noreply, %{state | timer: timer_ref}}
  end
end
