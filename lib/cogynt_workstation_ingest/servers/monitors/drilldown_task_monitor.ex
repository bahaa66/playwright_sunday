defmodule CogyntWorkstationIngest.Servers.DrilldownTaskMonitor do
  @moduledoc """
    Module that monitors the the status of Drilldown Tasks.
    Will publish via pub/sub when task is completed and store
    status of task in Redis.
  """

  use GenServer

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def monitor(pid) do
    GenServer.cast(__MODULE__, {:monitor, pid})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:monitor, pid}, state) do
    Process.monitor(pid)

    Redis.hash_set("drilldown_task", "task_status", true)
    Redis.hash_set("drilldown_task", "pid", pid)
    Redis.key_pexpire("drilldown_task", 30000)

    Redis.publish("drilldown_task_status_subscription", %{deleting: true})

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # TODO implement retry for backfill/update task if reason anything other than :normal or :shutdown

    Redis.key_delete("drilldown_task")

    Redis.publish_async("drilldown_task_status_subscription", %{deleting: false})

    {:noreply, state}
  end
end
