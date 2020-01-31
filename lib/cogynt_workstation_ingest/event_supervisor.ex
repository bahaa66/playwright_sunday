defmodule CogyntWorkstationIngest.EventSupervisor do
  use DynamicSupervisor

  alias CogyntWorkstationIngest.EventPipeline

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end


  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_child(event_definition) do
    child_spec = %{
      id: event_definition.topic,
      start: {
        EventPipeline,
        :start_link,
        [{:event_definition, event_definition}]
      },
      restart: :transient,
      shutdown: 5000,
      type: :supervisor
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end


  def stop_child(topic) do
    child_name = String.to_atom("#{EventPipeline}#{topic}")
    child_pid = Process.whereis(child_name)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    end
  end
end
