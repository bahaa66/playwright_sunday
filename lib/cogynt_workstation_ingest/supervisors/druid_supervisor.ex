defmodule CogyntWorkstationIngest.Supervisors.DruidSupervisor do
  use DynamicSupervisor

  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor

  def start_link(arg),
    do: DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)

  def init(_arg),
    do: DynamicSupervisor.init(strategy: :one_for_one)

  def create_druid_supervisor(arg),
    do: DynamicSupervisor.start_child(__MODULE__, {SupervisorMonitor, arg})
end
