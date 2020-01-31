defmodule CogyntWorkstationIngest.EventPipeline do
  use Broadway

  alias Broadway.Message
  alias CogyntWorkstationIngest.EventProducer

  def start_link({:event_definition, event_definition} = args) do
    name = String.to_atom("#{__MODULE__}#{event_definition.topic}")

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
          stages: 2
        ]
      ]
    )
  end

  def transform(event, opts) do
    #IO.inspect(event, label: "@@@ Transformaton Event")
    #IO.inspect(opts[:event_definition], label: "@@@ Transformation opts")

    %Message{
      data: %{event: event, event_definition: opts[:event_definition]},
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  def ack(:ack_id, _successful, _failed) do
    # Write ack code here
    # IO.inspect(successful, label: "@@@ Success: ")
    # IO.inspect(failed, label: "@@@ Failed: ")
  end

  @impl true
  def handle_message(_, %Message{data: _data} = message, _) do
    # This is handling a single message that is being sent from the
    # Producer
    # IO.inspect(message, label: "@@@ Handle Message data: ")
    IO.inspect(message, label: "@@@ Handle_message data")
  end

  # defp process_data(_data) do
  # end
end
