defmodule CogyntWorkstationIngest.Servers.DeploymentTaskMonitor do
  @moduledoc """
    Module that monitors the the status of Deployment Tasks.
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

    Redis.hash_set("deployment_task", "task_status", true)
    Redis.hash_set("deployment_task", "pid", pid)
    Redis.key_pexpire("deployment_task", 30000)

    Redis.publish("deployment_task_status_subscription", %{deleting: true})

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # TODO implement retry for backfill/update task if reason anything other than :normal or :shutdown

    Redis.key_delete("deployment_task")

    Redis.publish_async("deployment_task_status_subscription", %{deleting: false})

    {:noreply, state}
  end
end
