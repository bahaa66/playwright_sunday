defmodule CogyntWorkstationIngest.Broadway.EventPipeline do
  use Broadway

  alias Broadway.Message
  alias CogyntWorkstationIngest.Broadway.{EventProducer, EventProcessor}

  def start_link({:event_definition, event_definition} = args) do
    name = String.to_atom("BroadwayEventPipeline-#{event_definition.topic}")

    producer_args = [
      {:cache_key, "#{event_definition.topic}_message_set"},
      {:name, name},
      {:table, :event_messages}
    ]

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {EventProducer, producer_args},
        stages: 1,
        transformer: {__MODULE__, :transform, [args]}
      ],
      processors: [
        default: [
          stages: 15,
          max_demand: 1000,
          min_demand: 100
        ]
      ]
    )
  end

  def transform(event, opts) do
    %Message{
      data: %{event: event, event_definition: opts[:event_definition]},
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  def ack(:ack_id, _successful, _failed) do
    # TODO Write ack code here
    # IO.inspect(successful, label: "@@@ Success: ")
    # IO.inspect(failed, label: "@@@ Failed: ")
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    result =
      data
      |> EventProcessor.process_event()
      |> EventProcessor.process_event_details()
      |> EventProcessor.process_notifications()
      |> EventProcessor.execute_transaction()

    #IO.inspect(result, label: "@@@ Handle_message result")
    message
  end
end
