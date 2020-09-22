defmodule CogyntWorkstationIngest.Broadway.DrilldownPipeline do
  @moduledoc """
  Broadway pipeline module for the DrilldownPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.DrilldownProcessor

  def start_link(%{group_id: group_id, topics: topics, hosts: hosts}) do
    Broadway.start_link(__MODULE__,
      name: String.to_atom(group_id <> "Pipeline"),
      producer: [
        module:
          {BroadwayKafka.Producer,
           [
             hosts: hosts,
             group_id: group_id,
             topics: topics,
             offset_commit_on_ack: true,
             offset_reset_policy: :earliest,
             group_config: [
               session_timeout_seconds: 15
             ],
             fetch_config: [
               # 3 MB
               max_bytes: 3_145_728
             ],
             client_config: [
               # 15 seconds
               connect_timeout: 15000
             ]
           ]},
        concurrency: 10,
        transformer: {__MODULE__, :transform, [group_id: group_id]}
      ],
      processors: [
        default: [
          concurrency: Config.drilldown_processor_stages()
        ]
      ],
      context: [group_id: group_id]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, opts) do
    group_id = Keyword.get(opts, :group_id, 1)

    case Jason.decode(encoded_data) do
      {:ok, decoded_data} ->
        # Incr the total message count that has been consumed from kafka
        Redis.hash_increment_by("dmi:#{group_id}", "tmc", 1)

        Map.put(message, :data, %{event: decoded_data})

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode Kafka message. Error: #{inspect(error)}"
        )

        Map.put(message, :data, nil)
    end
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _opts) do
    CogyntLogger.error("#{__MODULE__}", "Messages failed. #{inspect(messages)}")
    # TODO: handle failed messages
    # DrilldownProducer.enqueue_failed_messages(messages)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_template_data/1 and update_cache/1
  """
  @impl true
  def handle_message(_processor, message, context) do
    group_id = Keyword.get(context, :group_id, 1)

    message
    |> DrilldownProcessor.process_template_data()
    |> DrilldownProcessor.upsert_template_solutions()

    {:ok, _tmp} = Redis.hash_increment_by("dmi:#{group_id}", "tmp", 1)

    message
  end
end
