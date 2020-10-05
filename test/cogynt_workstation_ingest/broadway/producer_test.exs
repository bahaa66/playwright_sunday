defmodule CogyntWorkstationIngest.Broadway.ProducerTest do
  use ExUnit.Case
  alias CogyntWorkstationIngest.Broadway.Producer

  @event_definition %{
    active: true,
    authoring_event_definition_id: "3171f670-f4fc-11e9-a42c-8c85900cbd75",
    created_at: "2020-02-24T19:13:35Z",
    deleted_at: nil,
    event_type: "entity",
    fields: %{
      "atm_id" => "string",
      "atm_name" => "string",
      "city" => "string",
      "coordinate" => "geo",
      "district" => "string",
      "state" => "string",
      "zip_code" => "integer"
    },
    id: "b61dbd12-5918-4fe9-ab55-a2c48551c167",
    primary_title_attribute: nil,
    title: "atm_locations_entity",
    topic: "atm_locations_entities",
    updated_at: "2020-02-24T19:13:35Z"
  }

  describe "handle_cast/2 enqueue" do
    test "enqueue kafka messages to Broadway Producer" do
      {:producer, state} = Producer.init(:ok)

      {:noreply, messages, state} =
        Producer.handle_cast({:enqueue, @kafka_messages, @event_definition}, state)

      assert messages == []
      assert state.demand == 0
      assert state.failed_messages == []

      queues = state.queues
      queue = Map.get(queues, @event_definition.id)

      assert :queue.len(queue) == 3
    end
  end

  describe "handle_cast/2 enqueue with demand" do
    test "enqueue kafka messages to Broadway Producer with pending demand" do
      {:producer, state} = Producer.init(:ok)

      state = Map.put(state, :demand, Enum.count(@kafka_messages))

      {:noreply, messages, state} =
        Producer.handle_cast({:enqueue, @kafka_messages, @event_definition}, state)

      assert Enum.count(messages) == Enum.count(@kafka_messages)
      assert state.demand == 0
      assert state.failed_messages == []

      queues = state.queues
      queue = Map.get(queues, @event_definition.id)

      assert :queue.len(queue) == 0
    end
  end

  describe "handle_cast/2 drain_queue" do
    test "empty the queue for the event_definition_id" do
      {:producer, state} = Producer.init(:ok)

      {:noreply, _messages, state} =
        Producer.handle_cast({:enqueue, @kafka_messages, @event_definition}, state)

      queues = state.queues
      queue = Map.get(queues, @event_definition.id)

      assert :queue.len(queue) == 3

      {:noreply, _messages, state} =
        Producer.handle_cast({:drain_queue, @event_definition.id}, state)

      queues = state.queues
      result = Map.has_key?(queues, @event_definition.id)

      assert result == false
    end
  end

  describe "handle_demand/2" do
    test "send an incoming demand to the producer" do
      {:producer, state} = Producer.init(:ok)

      {:noreply, _messages, state} =
        Producer.handle_cast({:enqueue, @kafka_messages, @event_definition}, state)

      {:noreply, messages, state} = Producer.handle_demand(10, state)

      assert Enum.count(messages) == Enum.count(@kafka_messages)
      assert state.demand == 10 - Enum.count(@kafka_messages)
      assert state.failed_messages == []
      queues = state.queues
      queue = Map.get(queues, @event_definition.id)

      assert :queue.len(queue) == 0
    end
  end
end
