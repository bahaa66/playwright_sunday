defmodule CogyntWorkstationIngest.Supervisors.TaskSupervisor do
  @moduledoc """
  Supervisor for all CogyntWorkstationIngest modules that implement Task.
  """
  use Supervisor
  alias CogyntWorkstationIngest.Utils.Tasks.{StartUpTask, CreateIngestionTasksConsumerGroup}

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      {StartUpTask, []},
      {CreateIngestionTasksConsumerGroup, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
