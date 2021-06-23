defmodule CogyntWorkstationIngest.Broadway.EventPipeline do
  @moduledoc """
  Broadway pipeline module for the EventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias Models.Enums.ConsumerStatusTypeEnum

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Broadway.{EventProcessor, LinkEventProcessor}

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]

  def start_link(%{
        group_id: group_id,
        topics: topics,
        hosts: hosts,
        event_definition_id: event_definition_id
      }) do
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
               session_timeout_seconds: 30
             ],
             client_config: [
               connect_timeout: 30000
             ]
           ]},
        concurrency: Config.event_producer_stages(),
        transformer: {__MODULE__, :transform, [event_definition_id: event_definition_id]}
      ],
      processors: [
        default: [
          concurrency: Config.event_processor_stages()
        ]
      ],
      batchers: [
        default: [
          batch_size: 600,
          concurrency: 10
        ]
      ],
      context: [event_definition_id: event_definition_id]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, opts) do
    event_definition_id = Keyword.get(opts, :event_definition_id, nil)

    if is_nil(event_definition_id) do
      CogyntLogger.error(
        "#{__MODULE__}",
        "event_definition_id not passed to EventPipeline."
      )

      Map.put(message, :data, nil)
    else
      case Jason.decode(encoded_data) do
        {:ok, decoded_data} ->
          # Incr the total message count that has been consumed from kafka
          incr_total_fetched_message_count(event_definition_id)

          Map.put(message, :data, %{
            event: decoded_data,
            event_definition_id: event_definition_id,
            core_id: nil,
            pipeline_state: nil,
            retry_count: 0,
            event_definition:
              EventsContext.get_event_definition(event_definition_id, preload_details: true)
              |> EventsContext.remove_event_definition_virtual_fields(
                include_event_definition_details: true
              )
          })

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to decode EventPipeline Kafka message. Error: #{inspect(error)}"
          )

          Map.put(message, :data, nil)
      end
    end
  end

  @doc """
  Acknowledgment callback only triggered for when failed messages are republished
  through the pipeline
  """
  def ack(:ack_id, _successful, _failed) do
    :ok
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, context) do
    event_definition_id = Keyword.get(context, :event_definition_id, nil)

    incr_total_processed_message_count(event_definition_id, Enum.count(messages))

    failed_messages =
      Enum.reduce(messages, [], fn %Broadway.Message{
                                     data:
                                       %{
                                         event_definition_id: id,
                                         retry_count: retry_count
                                       } = data
                                   } = message,
                                   acc ->
        if retry_count < Config.failed_messages_max_retry() do
          new_retry_count = retry_count + 1

          CogyntLogger.warn(
            "#{__MODULE__}",
            "Retrying Failed EventPipeline Message. EventDefinitionId: #{id}. Attempt: #{
              new_retry_count
            }"
          )

          data = Map.put(data, :retry_count, new_retry_count)

          message =
            Map.from_struct(message)
            |> Map.put(:data, data)
            |> Map.drop([:status, :acknowledger])

          metadata =
            Map.get(message, :metadata)
            |> Map.put(:key, "")

          message = Map.put(message, :metadata, metadata)

          acc ++ [message]
        else
          acc
        end
      end)

    key =
      if is_nil(event_definition_id) do
        "fem:"
      else
        "fem:#{event_definition_id}"
      end

    case Redis.list_length(key) do
      {:ok, length} when length >= 50_000 ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed Event messages for key #{key} have reached the limit of 50_000 in Redis. Dropping future messages from getting queued"
        )

      _ ->
        Redis.list_append_pipeline(key, failed_messages)
        # 30 min TTL
        Redis.key_pexpire(key, 1_800_000)
    end

    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_event/1, process_event_details_and_elasticsearch_docs/1,
  process_notifications/1 and execute_transaction/1.
  """
  @impl true
  def handle_message(_processor, %Message{data: nil} = message, _context), do: message

  @impl true
  def handle_message(
        _processor,
        %Message{
          data: %{
            event_definition: %{event_type: :linkage},
            pipeline_state: pipeline_state
          }
        } = message,
        _context
      ) do
    message =
      case pipeline_state do
        :process_event ->
          message
          |> EventProcessor.process_event_details_and_elasticsearch_docs()
          |> EventProcessor.process_notifications()
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()

        :process_event_details_and_elasticsearch_docs ->
          message
          |> EventProcessor.process_notifications()
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()

        :process_notifications ->
          message
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()

        :validate_link_event ->
          message
          |> LinkEventProcessor.process_entities()

        _ ->
          message
          |> EventProcessor.process_event()
          |> EventProcessor.process_event_details_and_elasticsearch_docs()
          |> EventProcessor.process_notifications()
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()
      end

    message
  end

  @impl true
  def handle_message(
        _processor,
        %Message{data: %{pipeline_state: pipeline_state}} = message,
        _context
      ) do
    message =
      case pipeline_state do
        :process_event ->
          message
          |> EventProcessor.process_event_details_and_elasticsearch_docs()
          |> EventProcessor.process_notifications()

        :process_event_details_and_elasticsearch_docs ->
          message
          |> EventProcessor.process_notifications()

        _ ->
          message
          |> EventProcessor.process_event()
          |> EventProcessor.process_event_details_and_elasticsearch_docs()
          |> EventProcessor.process_notifications()
      end

    message
  end

  @impl true
  def handle_batch(_, messages, _batch_info, context) do
    event_definition_id = Keyword.get(context, :event_definition_id, nil)

    List.first(messages)
    |> case do
      %Message{data: %{event: %{@crud => _action}}} ->
        messages
        |> Enum.group_by(fn message -> message.data.event["id"] end)
        |> EventProcessor.execute_batch_transaction_for_crud()

      _ ->
        messages
        |> Enum.map(fn message -> message.data end)
        |> EventProcessor.execute_batch_transaction()
    end

    incr_total_processed_message_count(event_definition_id, Enum.count(messages))
    messages
  end

  @doc false
  def pipeline_started?(event_definition_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id) <> "Pipeline")
    |> String.to_atom()
    |> Process.whereis()
    |> case do
      nil ->
        false

      _ ->
        true
    end
  end

  @doc false
  def pipeline_running?(event_definition_id) do
    if pipeline_started?(event_definition_id) do
      (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id) <> "Pipeline")
      |> String.to_atom()
      |> Broadway.producer_names()
      |> Enum.reduce(true, fn producer, acc ->
        case GenStage.demand(producer) do
          :forward ->
            acc and true

          :accumulate ->
            acc and false
        end
      end)
    else
      false
    end
  end

  @doc false
  # TODO: look into GenStage.estimate_buffered_count(stage, timeout \\ 5000)
  # to see if we can replace the redis message_info key with this.
  def pipeline_finished_processing?(event_definition_id) do
    case Redis.key_exists?("emi:#{event_definition_id}") do
      {:ok, false} ->
        true

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("emi:#{event_definition_id}", "tmc")
        {:ok, tmp} = Redis.hash_get("emi:#{event_definition_id}", "tmp")

        if is_nil(tmc) or is_nil(tmp) do
          CogyntLogger.info(
            "#{__MODULE__}",
            "TMC or TMP returned NIL, key has expired. EventDefinitionId: #{event_definition_id}"
          )

          true
        else
          String.to_integer(tmp) >= String.to_integer(tmc)
        end
    end
  end

  @doc false
  def suspend_pipeline(event_definition_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id) <> "Pipeline")
    |> String.to_atom()
    |> Broadway.producer_names()
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :accumulate)
    end)
  end

  @doc false
  def resume_pipeline(event_definition_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id) <> "Pipeline")
    |> String.to_atom()
    |> Broadway.producer_names()
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :forward)
    end)
  end

  @doc false
  def estimated_buffer_count(event_definition_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id) <> "Pipeline")
    |> String.to_atom()
    |> Broadway.producer_names()
    |> Enum.reduce(0, fn producer, acc ->
      acc + GenStage.estimate_buffered_count(producer)
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp finished_processing(event_definition_id) do
    {:ok, %{status: status, topic: topic}} =
      ConsumerStateManager.get_consumer_state(event_definition_id)

    if status == ConsumerStatusTypeEnum.status()[:paused_and_processing] do
      ConsumerStateManager.upsert_consumer_state(event_definition_id,
        topic: topic,
        status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
      )
    end

    Redis.publish_async("event_definitions_subscription", %{updated: event_definition_id})

    CogyntLogger.info(
      "#{__MODULE__}",
      "EventPipeline finished processing messages for EventDefinitionId: #{event_definition_id}"
    )
  end

  defp incr_total_fetched_message_count(event_definition_id) do
    Redis.hash_increment_by("emi:#{event_definition_id}", "tmc", 1)
    Redis.key_pexpire("emi:#{event_definition_id}", 60000)
  end

  defp incr_total_processed_message_count(event_definition_id, count) do
    {:ok, tmc} = Redis.hash_get("emi:#{event_definition_id}", "tmc")
    {:ok, tmp} = Redis.hash_increment_by("emi:#{event_definition_id}", "tmp", count)
    Redis.key_pexpire("emi:#{event_definition_id}", 60000)

    if is_nil(tmc) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "TMC returned NIL, key has expired. EventDefinitionId: #{event_definition_id}"
      )
    else
      if tmp >= String.to_integer(tmc) do
        finished_processing(event_definition_id)
      end
    end
  end
end
