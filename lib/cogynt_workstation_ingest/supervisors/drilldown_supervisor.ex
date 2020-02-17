defmodule CogyntWorkstationIngest.Supervisors.DrilldownSupervisor do
  @moduledoc """
  DymanicSupervisor module for Broadway DrilldownPipeline. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Broadway.DrilldownPipeline

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Will start a Broadway DrilldownPipeline
  """
  def start_child() do
    child_spec = %{
      id: :BroadwayDrilldown,
      start: {
        DrilldownPipeline,
        :start_link,
        []
      },
      restart: :transient,
      shutdown: 5000,
      type: :supervisor
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Will stop the Broadway DrilldownPipeline for the topic
  """
  def stop_child do
    child_pid = Process.whereis(:BroadwayDrilldown)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    else
      :ok
    end
  end
end
