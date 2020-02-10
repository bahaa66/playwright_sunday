defmodule CogyntWorkstationIngest.Broadway.EventPipeline do
  @moduledoc """
  Broadway pipeline module for the EventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway

  alias Broadway.Message
  alias CogyntWorkstationIngest.Broadway.{EventProducer, EventProcessor}

  def start_link({:event_definition, event_definition} = args) do
    name = String.to_atom("BroadwayEventPipeline-#{event_definition.topic}")
    # Optional args to pass to the init of the producer
    producer_args = []

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [
        module: {EventProducer, producer_args},
        stages: 1,
        transformer: {__MODULE__, :transform, [args]}
      ],
      processors: [
        default: [
          stages: processor_stages(),
          max_demand: processor_max_demand(),
          min_demand: processor_min_demand()
        ]
      ],
      partition_by: &partition/1,
      context: args
    )
  end

  defp partition(msg) do
    case msg.data.event["published_by"] do
      nil ->
        :rand.uniform(1000)

      id ->
        :erlang.phash2(id)
    end
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(event, opts) do
    %Message{
      data: %{event: event, event_definition: opts[:event_definition]},
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  @doc """
  Acknowledge callback. Will get all success or failed messages from
  the pipeline.
  """
  def ack(:ack_id, _successful, _failed) do
    IO.puts("Ack'd")
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, {:event_definition, event_definition}) do
    IO.puts("Failed")
    EventProducer.enqueue_failed_messages(messages, event_definition.topic)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_event/1, process_event_details_and_elasticsearch_docs/1,
  process_notifications/1 and execute_transaction/1.
  """
  @impl true
  def handle_message(_processor, %Message{data: data} = message, _context) do
    data
    |> EventProcessor.process_event()
    |> EventProcessor.process_event_details_and_elasticsearch_docs()
    |> EventProcessor.process_notifications()
    |> EventProcessor.execute_transaction()

    message
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp processor_stages(), do: config()[:processor_stages]
  defp processor_max_demand(), do: config()[:processor_max_demand]
  defp processor_min_demand(), do: config()[:processor_min_demand]
end
