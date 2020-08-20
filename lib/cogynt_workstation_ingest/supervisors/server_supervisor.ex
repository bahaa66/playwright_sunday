defmodule CogyntWorkstationIngest.Supervisors.ServerSupervisor do
  @moduledoc """
  Supervisor for all CogyntWorkstationIngest GenServer modules
  """
  use Supervisor

  alias CogyntWorkstationIngest.Servers.Caches.{
    ConsumerRetryCache,
    DeploymentConsumerRetryCache,
    NotificationSubscriptionCache
  }

  alias CogyntWorkstationIngest.Servers.PubSub.{
    IngestPubSub
  }

  alias CogyntWorkstationIngest.Servers.{
    Startup,
    ConsumerMonitor,
    NotificationsTaskMonitor
  }

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Start Redis pub/sub
    {:ok, pubsub} = Redis.pub_sub_start()

    children = [
      child_spec(ConsumerRetryCache),
      child_spec(NotificationSubscriptionCache),
      child_spec(DeploymentConsumerRetryCache),
      child_spec(Startup),
      child_spec(ConsumerMonitor, restart: :permanent),
      child_spec(NotificationsTaskMonitor, restart: :permanent),
      child_spec(IngestPubSub, start_link_opts: [pubsub])
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp child_spec(module_name, opts \\ []) do
    restart_type = Keyword.get(opts, :restart, :transient)
    start_link_opts = Keyword.get(opts, :start_link_opts, [])

    %{
      id: module_name,
      start: {
        module_name,
        :start_link,
        start_link_opts
      },
      restart: restart_type,
      shutdown: 5000,
      type: :worker
    }
  end
end
