defmodule CogyntWorkstationIngest.Broadway.LinkEventPipeline do
  @moduledoc """
  Broadway pipeline module for the LinkEventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  require Logger
  alias Broadway.Message
  alias CogyntWorkstationIngest.Broadway.{Producer, LinkEventProcessor, EventProcessor}

  def start_link(_args) do
    Broadway.start_link(__MODULE__,
      name: :BroadwayLinkEventPipeline,
      producer: [
        module: {Producer, []},
        stages: 1,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [
          stages: processor_stages(),
          max_demand: processor_max_demand(),
          min_demand: processor_min_demand()
        ]
      ],
      partition_by: &partition/1
    )
  end

  defp partition(msg) do
    case msg.data.event["id"] do
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
    IO.puts("Ack'd")
  end

  @doc """
  Callback for handling any failed messages in the LinkEventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _args) do
    IO.puts("Failed")
    Producer.enqueue_failed_messages(messages, :linkevent)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  process_entity_ids/1, process_entity_ids/1, process_event/1,
  process_event_details_and_elasticsearch_docs/1, process_notifications/1,
  process_event_links/1 and execute_transaction/1.
  """
  @impl true
  def handle_message(
        _processor,
        %Message{data: %{event_processed: false} = data} = message,
        _context
      ) do
    pipeline_state =
      data
      |> EventProcessor.process_event()
      |> EventProcessor.process_event_details_and_elasticsearch_docs()
      |> EventProcessor.process_notifications()
      |> EventProcessor.execute_transaction()
      |> LinkEventProcessor.validate_link_event()
      |> LinkEventProcessor.process_entities()
      |> LinkEventProcessor.process_entity_ids()
      |> LinkEventProcessor.process_event_links()
      |> LinkEventProcessor.execute_transaction()

    check_for_failure_with_state(message, pipeline_state)
  end

  @impl true
  def handle_message(
        _processor,
        %Message{data: %{event_processed: true} = data} = message,
        _context
      ) do
    pipeline_state =
      data
      |> LinkEventProcessor.validate_link_event()
      |> LinkEventProcessor.process_entities()
      |> LinkEventProcessor.process_entity_ids()
      |> LinkEventProcessor.process_event_links()
      |> LinkEventProcessor.execute_transaction()

    check_for_failure_with_state(message, pipeline_state)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp check_for_failure_with_state(message, state) do
    case Map.get(state, :link_event_ready) do
      false ->
        Map.put(message, :data, state)
        |> Message.failed("LinkEvent is not ready for processing. Entity events DNE")

      _ ->
        message
    end
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, __MODULE__)
  defp processor_stages(), do: config()[:processor_stages]
  defp processor_max_demand(), do: config()[:processor_max_demand]
  defp processor_min_demand(), do: config()[:processor_min_demand]
end
