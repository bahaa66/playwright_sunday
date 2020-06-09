defmodule CogyntWorkstationIngest.Broadway.EventPipeline do
  @moduledoc """
  Broadway pipeline module for the EventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.{Producer, EventProcessor}

  @pipeline_name :BroadwayEventPipeline

  def start_link(_args) do
    Broadway.start_link(__MODULE__,
      name: @pipeline_name,
      producer: [
        module: {Producer, []},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []},
        rate_limiting: [
          allowed_messages: Config.producer_allowed_messages(),
          interval: Config.producer_rate_limit_interval()
        ]
      ],
      processors: [
        default: [
          concurrency: Config.event_processor_stages(),
          max_demand: Config.event_processor_max_demand(),
          min_demand: Config.event_processor_min_demand()
        ]
      ],
      partition_by: &partition/1
    )
  end

  defp partition(msg) do
    case msg.data.event["id"] do
      nil ->
        :rand.uniform(100_000)

      id ->
        :erlang.phash2(id)
    end
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :ack_id, :ack_data}
    }
  end

  @doc """
  Acknowledge callback. Will get all success or failed messages from
  the pipeline.
  """
  def ack(:ack_id, _successful, _failed) do
    # CogyntLogger.info("#{__MODULE__}", "Messages Ackd.")
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _args) do
    # CogyntLogger.error("#{__MODULE__}", "Messages failed. #{inspect(messages)}")
    Producer.enqueue_failed_messages(messages, @pipeline_name)
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
end
