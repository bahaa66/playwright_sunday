defmodule CogyntWorkstationIngest.Supervisors.DynamicTaskSupervisor do
  @moduledoc """
  Dynamic Supervisor for all CogyntWorkstationIngest modules that implement Task.
  """
  use DynamicSupervisor

  alias CogyntWorkstationIngest.Servers.{
    EventDefinitionTaskMonitor,
    DrilldownTaskMonitor,
    DeploymentTaskMonitor
  }

  alias CogyntWorkstationIngest.Utils.Tasks.{
    DeleteDrilldownDataTask,
    DeleteEventDefinitionsAndTopicsTask,
    DeleteDeploymentDataTask
  }

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Based on the args passed in will start the module to execute the async task to execute the
  work defined within the module
  """
  def start_child(args) do
    Enum.each(args, fn
      {:delete_drilldown_data, delete_drilldown_topics} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteDrilldownDataTask, delete_drilldown_topics}
          )

        DrilldownTaskMonitor.monitor(pid)
        {:ok, pid}

      {:delete_deployment_data, true} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            DeleteDeploymentDataTask
          )

        DeploymentTaskMonitor.monitor(pid)
        {:ok, pid}

      {:delete_event_definitions_and_topics, %{event_definition_ids: event_definition_ids} = args} ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            __MODULE__,
            {DeleteEventDefinitionsAndTopicsTask, args}
          )

        EventDefinitionTaskMonitor.monitor(pid, event_definition_ids)
        {:ok, pid}

      _ ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Invalid args passed. Args: #{inspect(args, pretty: true)}"
        )
    end)
  end
end
