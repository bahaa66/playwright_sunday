defmodule CogyntWorkstationIngest.Supervisors.LinkEventSupervisor do
  @moduledoc """
  DymanicSupervisor module for Broadway LinkEventPipeline. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Broadway.LinkEventPipeline

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Will start a Broadway LinkEventPipeline for the event_definition.topic
  """
  def start_child(event_definition) do
    child_spec = %{
      id: event_definition.topic,
      start: {
        LinkEventPipeline,
        :start_link,
        [{:event_definition, event_definition}]
      },
      restart: :transient,
      shutdown: 5000,
      type: :supervisor
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Will stop the Broadway LinkEventPipeline for the topic
  """
  def stop_child(topic) do
    child_name = String.to_atom("BroadwayLinkEventPipeline-#{topic}")
    child_pid = Process.whereis(child_name)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    else
      :ok
    end
  end
end
