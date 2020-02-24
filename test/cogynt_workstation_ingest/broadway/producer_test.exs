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

  @kafka_messages [
    %KafkaEx.Protocol.Fetch.Message{
      attributes: 0,
      crc: 1_816_133_476,
      key: nil,
      offset: 0,
      value:
        "{\"atm_name\":\"ID_247506870\",\"coordinate\":{\"geometry\":{\"coordinates\":[-122.26778600000002,37.8069224],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"name\":\"Point\"}},\"city\":\"Oakland\",\"publishing_template_type\":\"516bccbe-f4ff-11e9-b63b-8c85900cbd75\",\"latency\":35,\"$crud\":\"create\",\"source\":\"3171f670-f4fc-11e9-a42c-8c85900cbd75\",\"zip_code\":94612,\"published_by\":\"3c742b8c-fd1f-384f-92d5-f2adcbbf197b\",\"publishing_template_type_name\":\"create_location_entities\",\"district\":\"Downtown\",\"data_type\":\"atm_locations_entity\",\"atm_id\":\"atm3\",\"state\":\"California\",\"id\":\"3c742b8c-fd1f-384f-92d5-f2adcbbf197b\",\"published_at\":\"2019-10-22T20:50:22.468Z\"}"
    },
    %KafkaEx.Protocol.Fetch.Message{
      attributes: 0,
      crc: 118_412_497,
      key: nil,
      offset: 1,
      value:
        "{\"atm_name\":\"ID_276234881\",\"coordinate\":{\"geometry\":{\"coordinates\":[-122.2707437,37.79998629999999],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"name\":\"Point\"}},\"city\":\"Oakland\",\"publishing_template_type\":\"516bccbe-f4ff-11e9-b63b-8c85900cbd75\",\"latency\":437,\"$crud\":\"create\",\"source\":\"3171f670-f4fc-11e9-a42c-8c85900cbd75\",\"zip_code\":94607,\"published_by\":\"f6639268-44d7-31f8-86c4-25bc77e3ccc4\",\"publishing_template_type_name\":\"create_location_entities\",\"district\":\"Chinatown\",\"data_type\":\"atm_locations_entity\",\"atm_id\":\"atm9\",\"state\":\"California\",\"id\":\"f6639268-44d7-31f8-86c4-25bc77e3ccc4\",\"published_at\":\"2019-10-22T20:50:22.873Z\"}"
    },
    %KafkaEx.Protocol.Fetch.Message{
      attributes: 0,
      crc: 2_245_031_157,
      key: nil,
      offset: 2,
      value:
        "{\"atm_name\":\"Wells Fargo\",\"coordinate\":{\"geometry\":{\"coordinates\":[-122.26002120000001,37.8684876],\"type\":\"Point\"},\"type\":\"Feature\",\"properties\":{\"name\":\"Point\"}},\"city\":\"Berkeley\",\"publishing_template_type\":\"516bccbe-f4ff-11e9-b63b-8c85900cbd75\",\"latency\":437,\"$crud\":\"create\",\"source\":\"3171f670-f4fc-11e9-a42c-8c85900cbd75\",\"zip_code\":94704,\"published_by\":\"48f53a12-e7a5-3e63-93a8-391c7714ac38\",\"publishing_template_type_name\":\"create_location_entities\",\"district\":\"Telegraph Avenue\",\"data_type\":\"atm_locations_entity\",\"atm_id\":\"atm11\",\"state\":\"California\",\"id\":\"48f53a12-e7a5-3e63-93a8-391c7714ac38\",\"published_at\":\"2019-10-22T20:50:22.874Z\"}"
    }
  ]

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
