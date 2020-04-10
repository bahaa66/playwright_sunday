defmodule CogyntWorkstationIngest.Supervisors.ServerSupervisor do
  @moduledoc """
  Supervisor for all CogyntWorkstationIngest GenServer modules
  """
  use Supervisor

  alias CogyntWorkstationIngest.Servers.Caches.{ConsumerRetryCache, DrilldownCache}
  alias CogyntWorkstationIngest.Servers.{Startup, ConsumerMonitor}

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      child_spec(ConsumerRetryCache),
      child_spec(DrilldownCache),
      child_spec(Startup),
      child_spec(ConsumerMonitor)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp child_spec(module_name) do
    %{
      id: module_name,
      start: {
        module_name,
        :start_link,
        []
      },
      restart: :transient,
      shutdown: 5000,
      type: :worker
    }
  end
end
