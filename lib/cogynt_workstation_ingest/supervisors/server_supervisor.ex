defmodule CogyntWorkstationIngest.Supervisors.ServerSupervisor do
  @moduledoc """
  Supervisor for all CogyntWorkstationIngest GenServer modules
  """
  use Supervisor

  alias CogyntWorkstationIngest.Servers.Caches.{
    ConsumerRetryCache,
    DrilldownCache,
    NotificationSubscriptionCache,
    EventProcessingCache
  }

  alias CogyntWorkstationIngest.Servers.{
    Startup,
    ConsumerMonitor,
    NotificationsTaskMonitor,
    ConsumerStateManager
  }

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      child_spec(ConsumerRetryCache),
      child_spec(DrilldownCache, restart: :permanent),
      child_spec(Startup),
      child_spec(ConsumerMonitor, restart: :permanent),
      child_spec(NotificationsTaskMonitor, restart: :permanent),
      child_spec(NotificationSubscriptionCache, restart: :permanent),
      child_spec(ConsumerStateManager, restart: :permanent),
      child_spec(EventProcessingCache, restart: :permanent)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp child_spec(module_name, opts \\ []) do
    restart_type = Keyword.get(opts, :restart, :transient)

    %{
      id: module_name,
      start: {
        module_name,
        :start_link,
        []
      },
      restart: restart_type,
      shutdown: 5000,
      type: :worker
    }
  end
end
