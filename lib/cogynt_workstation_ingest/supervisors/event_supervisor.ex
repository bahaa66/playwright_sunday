defmodule CogyntWorkstationIngest.Supervisors.EventSupervisor do
  @moduledoc """
  DymanicSupervisor module for Broadway EventPipeline. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Broadway.EventPipeline

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Will start a Broadway EventPipeline for the event_definition.topic
  """
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

  def start_children(event_definitions) do
    Enum.each(event_definitions, fn event_definition ->
      start_child(event_definition)
    end)
  end

  @doc """
  Will stop the Broadway EventPipeline for the topic
  """
  def stop_child(topic) do
    child_name = String.to_atom("BroadwayEventPipeline-#{topic}")
    child_pid = Process.whereis(child_name)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    else
      :ok
    end
  end

  def stop_children(topics) do
    Enum.each(topics, fn topic ->
      stop_child(topic)
    end)
  end
end
