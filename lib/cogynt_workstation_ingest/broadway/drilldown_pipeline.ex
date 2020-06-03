defmodule CogyntWorkstationIngest.Broadway.DrilldownPipeline do
  @moduledoc """
  Broadway pipeline module for the DrilldownPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.{DrilldownProducer, DrilldownProcessor}

  @pipeline_name :BroadwayDrilldown

  def start_link(_args) do
    Broadway.start_link(__MODULE__,
      name: @pipeline_name,
      producer: [
        module: {DrilldownProducer, []},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []},
        rate_limiting: [
          allowed_messages: Config.drilldown_producer_allowed_messages(),
          interval: Config.drilldown_producer_rate_limit_interval()
        ]
      ],
      processors: [
        default: [
          concurrency: Config.drilldown_processor_stages(),
          max_demand: Config.drilldown_processor_max_demand(),
          min_demand: Config.drilldown_processor_min_demand()
        ]
      ]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%{event: event, retry_count: retry_count}, _opts) do
    %Message{
      data: %{event: event, retry_count: retry_count},
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
  def handle_failed(messages, _opts) do
    CogyntLogger.error("#{__MODULE__}", "Messages failed. #{inspect(messages)}")
    DrilldownProducer.enqueue_failed_messages(messages)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_template_data/1 and update_cache/1
  """
  @impl true
  def handle_message(_processor, %Message{data: data} = message, _context) do
    data
    |> DrilldownProcessor.process_template_data()
    |> DrilldownProcessor.update_cache()

    message
  end
end
