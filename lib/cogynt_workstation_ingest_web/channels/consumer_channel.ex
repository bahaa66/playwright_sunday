defmodule CogyntWorkstationIngestWeb.Channels.ConsumerChannel do
  use Phoenix.Channel, hibernate_after: 60_000, log_join: :info, log_handle_in: :info

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  def join("ingest:consumers", _message, socket) do
    {:ok, socket}
  end

  def handle_in("consumer:start", event_definition, socket) do
    # start a consumer based on the event definition
    {:ok, pid} = ConsumerGroupSupervisor.start_child(event_definition)
    {:reply, {:ok, %{pid: pid}, socket}}
  end

  def handle_in("consumer:start_async", event_definition, socket) do
    # start a consumer based on the event definition
    ConsumerGroupSupervisor.start_child(event_definition)
    {:noreply, socket}
  end

  def handle_in("consumer:stop", %{topic: topic}, socket) do
    # stop the consumer based on the topic
    {:ok, pid} = ConsumerGroupSupervisor.stop_child(topic)
    {:reply, {:ok, %{pid: pid}, socket}}
  end

  def handle_in("consumer:stop_async", %{topic: topic}, socket) do
    # stop the consumer based on the topic
    ConsumerGroupSupervisor.stop_child(topic)
    {:noreply, socket}
  end
end
