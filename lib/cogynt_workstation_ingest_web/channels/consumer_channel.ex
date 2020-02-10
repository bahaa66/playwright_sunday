defmodule CogyntWorkstationIngestWeb.Channels.ConsumerChannel do
  use Phoenix.Channel, hibernate_after: 60_000, log_join: :info, log_handle_in: :info

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  def join("consumer:" <> topic, _message, socket) do
    IO.inspect(topic, label: "@@@ Joining Socket for topic")
    {:ok, socket}
  end

  def handle_in("consumer:start", event_definition, socket) do
    # start a consumer based on the event definition
    ConsumerGroupSupervisor.start_child(event_definition)
    {:reply, :ok, socket}
  end

  def handle_in("consumer:stop", %{topic: topic}, socket) do
    # stop the consumer based on the topic
    ConsumerGroupSupervisor.stop_child(topic)
    {:reply, :ok, socket}
  end
end
