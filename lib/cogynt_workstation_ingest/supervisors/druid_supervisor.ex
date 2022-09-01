defmodule CogyntWorkstationIngest.Supervisors.DruidSupervisor do
  # CURRENTLY NOT USED UNTIL WE START USING LIBCLUSTER AND HORDE REGISTRY AGAIN
  # NEED TO FIGURE OUT WHY THEY DONT WORK WITH ISTIO
  use Horde.DynamicSupervisor

  alias CogyntWorkstationIngest.Horde.HordeRegistry
  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor

  def start_link(_),
    do: Horde.DynamicSupervisor.start_link(__MODULE__, [strategy: :one_for_one], name: __MODULE__)

  def init(init_arg) do
    [members: members()]
    |> Keyword.merge(init_arg)
    |> Horde.DynamicSupervisor.init()
  end

  def start_child(opts) do
    name =
      opts
      |> Keyword.get(:name, SupervisorMonitor)
      |> via_tuple()

    new_opts = Keyword.put(opts, :name, name)

    child_spec = %{
      id: SupervisorMonitor,
      start: {SupervisorMonitor, :start_link, [new_opts]},
      restart: :transient
    }

    Horde.DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  def whereis(name \\ SupervisorMonitor) do
    name
    |> via_tuple()
    |> GenServer.whereis()
  end

  defp members() do
    Enum.map([Node.self() | Node.list()], &{__MODULE__, &1})
  end

  defp via_tuple(name) do
    {:via, Horde.Registry, {HordeRegistry, name}}
  end
end
