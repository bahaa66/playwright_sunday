defmodule CogyntWorkstationIngest.Broadway.DrilldownPipeline do
  @moduledoc """
  Broadway pipeline module for the DrilldownPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Broadway.{DrilldownProducer, DrilldownProcessor}

  def start_link() do
    Broadway.start_link(__MODULE__,
      name: :BroadwayDrilldown,
      producer: [
        module: {DrilldownProducer, []},
        stages: 1,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [
          stages: processor_stages(),
          max_demand: processor_max_demand(),
          min_demand: processor_min_demand()
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
    IO.puts("Ack'd")
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _opts) do
    IO.puts("Failed")
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

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp processor_stages(), do: config()[:processor_stages]
  defp processor_max_demand(), do: config()[:processor_max_demand]
  defp processor_min_demand(), do: config()[:processor_min_demand]
end
