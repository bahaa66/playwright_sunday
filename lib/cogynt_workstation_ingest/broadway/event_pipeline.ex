defmodule CogyntWorkstationIngest.Broadway.EventPipeline do
  @moduledoc """
  Broadway pipeline module for the EventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Broadway.{EventProcessor, LinkEventProcessor}
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias Kafka.Api.Topic
  alias Models.Enums.ConsumerStatusTypeEnum

  def start_link(%{
        group_id: group_id,
        topics: topics,
        hosts: hosts,
        event_definition_hash_id: event_definition_hash_id,
        event_type: event_type
      }) do
    concurrency = calc_pipeline_concurrency(topics, hosts, group_id)

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
             ],
             fetch_config: [
               max_bytes: Config.event_pipeline_max_bytes()
             ]
           ]},
        concurrency: concurrency,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [
          concurrency: concurrency
        ]
      ],
      # TODO: Look into whether we need to have two batchers. If pipeline will only use
      # one or the other then we could just use the :default batcher and use the crud
      # keys to determine how the events are processed and handled. The only reason I
      # could see having two is if there were ever mixed kafka topics.
      batchers: [
        default: [
          batch_size: Config.event_pipeline_batch_size(),
          batch_timeout: Config.event_pipeline_batch_timeout(),
          concurrency: concurrency
        ],
        crud: [
          batch_size: Config.event_pipeline_batch_size(),
          batch_timeout: Config.event_pipeline_batch_timeout(),
          concurrency: concurrency
        ]
      ],
      context: [
        event_definition_hash_id: event_definition_hash_id,
        event_type: event_type
      ]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, _context) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    case Jason.decode(encoded_data, keys: :atoms) do
      {:ok, decoded_data} ->
        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :transform],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        Message.put_data(message, %{kafka_event: decoded_data, pipeline_state: nil})

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode EventPipeline Kafka message. Error: #{inspect(error)}"
        )

        Message.failed(message, :json_decode_error)
    end
  end

  @impl true
  def prepare_messages(messages, context) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    # updates the total message counter fetched from kafka
    incr_total_fetched_message_count(
      Keyword.get(context, :event_definition_hash_id),
      Enum.count(messages)
    )

    event_definition =
      EventsContext.get_event_definition(
        Keyword.get(context, :event_definition_hash_id),
        preload_details: true,
        preload_notification_settings: true
      )

    messages =
      Enum.map(messages, fn m ->
        Message.update_data(m, &Map.put(&1, :event_definition, event_definition))
      end)

    # Execute telemtry for metrics
    :telemetry.execute(
      [:broadway, :prepare_messages],
      %{duration: System.monotonic_time() - start},
      telemetry_metadata
    )

    messages
  end

  @impl true
  def handle_message(
        _processor_name,
        %{data: %{kafka_event: k_event}} = message,
        context
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}
    crud = not is_nil(Map.get(k_event, String.to_atom(Config.crud_key())))
    core_id = Map.get(k_event, String.to_atom(Config.id_key()), Ecto.UUID.generate())

    if(
      crud,
      do: Message.put_batcher(message, :crud),
      else: Message.put_batcher(message, :default)
    )
    |> Message.update_data(fn data ->
      data =
        Map.put(data, :core_id, core_id)
        |> Map.put(:event_type, Keyword.get(context, :event_type, "none"))

      data =
        case data.pipeline_state do
          :process_event ->
            data
            |> EventProcessor.process_event_history()
            |> LinkEventProcessor.validate_link_event()
            |> LinkEventProcessor.process_entities()
            |> EventProcessor.process_elasticsearch_documents()
            |> EventProcessor.process_notifications()

          :process_event_history ->
            data
            |> LinkEventProcessor.validate_link_event()
            |> LinkEventProcessor.process_entities()
            |> EventProcessor.process_elasticsearch_documents()
            |> EventProcessor.process_notifications()

          :validate_link_event ->
            data
            |> LinkEventProcessor.process_entities()
            |> EventProcessor.process_elasticsearch_documents()
            |> EventProcessor.process_notifications()

          :process_entities ->
            data
            |> EventProcessor.process_elasticsearch_documents()
            |> EventProcessor.process_notifications()

          :process_elasticsearch_documents ->
            data
            |> EventProcessor.process_notifications()

          _ ->
            data
            |> EventProcessor.process_event()
            |> EventProcessor.process_event_history()
            |> LinkEventProcessor.validate_link_event()
            |> LinkEventProcessor.process_entities()
            |> EventProcessor.process_elasticsearch_documents()
            |> EventProcessor.process_notifications()
        end

      # Execute telemtry for metrics
      :telemetry.execute(
        [
          :broadway,
          if(crud,
            do: :event_processor_all_crud_processing_stages,
            else: :event_processor_all_processing_stages
          )
        ],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

      data
    end)
  end

  @impl true
  def handle_batch(batch_type, messages, _batch_info, context) do
    IO.puts("------------------------------------------------")

    IO.inspect(Enum.count(messages),
      label: "#{String.upcase(Atom.to_string(batch_type))} BATCH COUNT"
    )

    # Enum.map(messages, fn message -> message.data end)
    # |> EventProcessor.execute_batch_transaction(
    #   batch_type == :crud,
    #   Keyword.get(context, :event_type)
    # )

    # increases the total message counter for messages processed
    incr_total_processed_message_count(
      Keyword.get(context, :event_definition_hash_id),
      Enum.count(messages)
    )

    messages
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _context) do
    CogyntLogger.warn(
      "#{__MODULE__}",
      "handle_failed/2 #{Enum.count(messages)} Failed for EventPipeline. Check logs to for error."
    )

    messages
  end

  @doc """
  Acknowledgment callback only triggered for when failed messages are republished
  through the pipeline
  """
  def ack(:ack_id, _successful, _failed) do
    :ok
  end

  @doc false
  def pipeline_started?(event_definition_hash_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_hash_id) <> "Pipeline")
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
  def pipeline_running?(event_definition_hash_id) do
    if pipeline_started?(event_definition_hash_id) do
      try do
        producer =
          (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_hash_id) <> "Pipeline")
          |> String.to_atom()
          |> Broadway.producer_names()
          |> List.first()

        case GenStage.call(producer, :"$demand", 120_000) do
          :forward ->
            true

          :accumulate ->
            false
        end
      rescue
        _ ->
          false
      end
    else
      false
    end
  end

  @doc false
  # TODO: look into GenStage.estimate_buffered_count(stage, timeout \\ 5000)
  # to see if we can replace the redis message_info key with this.
  def pipeline_finished_processing?(event_definition_hash_id) do
    case Redis.key_exists?("emi:#{event_definition_hash_id}") do
      {:ok, false} ->
        true

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("emi:#{event_definition_hash_id}", "tmc")
        {:ok, tmp} = Redis.hash_get("emi:#{event_definition_hash_id}", "tmp")

        if is_nil(tmc) or is_nil(tmp) do
          CogyntLogger.info(
            "#{__MODULE__}",
            "TMC or TMP returned NIL, key has expired. EventDefinitionHashId: #{event_definition_hash_id}"
          )

          true
        else
          String.to_integer(tmp) >= String.to_integer(tmc)
        end
    end
  end

  @doc false
  def suspend_pipeline(event_definition) do
    name = ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id)

    String.to_atom(name <> "Pipeline")
    |> Broadway.producer_names()
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :accumulate)
    end)
  end

  @doc false
  def resume_pipeline(event_definition) do
    name = ConsumerGroupSupervisor.fetch_event_cgid(event_definition.id)

    String.to_atom(name <> "Pipeline")
    |> Broadway.producer_names()
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :forward)
    end)
  end

  @doc false
  def estimated_buffer_count(event_definition_hash_id) do
    (ConsumerGroupSupervisor.fetch_event_cgid(event_definition_hash_id) <> "Pipeline")
    |> String.to_atom()
    |> Broadway.producer_names()
    |> Enum.reduce(0, fn producer, acc ->
      acc + GenStage.estimate_buffered_count(producer)
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp finished_processing(event_definition_hash_id) do
    {:ok, %{status: status, topic: topic}} =
      ConsumerStateManager.get_consumer_state(event_definition_hash_id)

    if status == ConsumerStatusTypeEnum.status()[:paused_and_processing] do
      ConsumerStateManager.upsert_consumer_state(event_definition_hash_id,
        topic: topic,
        status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
      )
    end

    Redis.publish_async("event_definitions_subscription", %{updated: event_definition_hash_id})

    CogyntLogger.info(
      "#{__MODULE__}",
      "EventPipeline finished processing messages for EventDefinitionHashId: #{event_definition_hash_id}"
    )
  end

  defp incr_total_fetched_message_count(event_definition_hash_id, count) do
    Redis.hash_increment_by("emi:#{event_definition_hash_id}", "tmc", count)
    Redis.key_pexpire("emi:#{event_definition_hash_id}", 60000)
  end

  defp incr_total_processed_message_count(event_definition_hash_id, count) do
    {:ok, tmc} = Redis.hash_get("emi:#{event_definition_hash_id}", "tmc")
    {:ok, tmp} = Redis.hash_increment_by("emi:#{event_definition_hash_id}", "tmp", count)
    Redis.key_pexpire("emi:#{event_definition_hash_id}", 60000)

    if is_nil(tmc) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "TMC returned NIL, key has expired. EventDefinitionHashId: #{event_definition_hash_id}"
      )
    else
      if tmp >= String.to_integer(tmc) do
        finished_processing(event_definition_hash_id)
      end
    end
  end

  defp calc_pipeline_concurrency(topics, brokers, consumer_group_id) do
    topic = List.first(topics)

    kafka_client_atom =
      try do
        String.to_existing_atom(consumer_group_id)
      rescue
        _error ->
          String.to_atom(consumer_group_id)
      end

    with :ok <- :brod.start_client(brokers, kafka_client_atom),
         :ok <- :brod.start_producer(kafka_client_atom, topic, []),
         {:ok, partition_count} <- Topic.fetch_partition_count(kafka_client_atom, topic) do
      # :brod.close_client(kafka_client_atom)

      if partition_count < Config.replicas() do
        partition_count
      else
        if Config.replicas() > 0 do
          round(partition_count / Config.replicas())
        else
          round(partition_count / 1)
        end
      end
    else
      :error ->
        # :brod.close_client(kafka_client_atom)

        CogyntLogger.warn(
          "#{__MODULE__}",
          "Stopped brod client for #{inspect(kafka_client_atom)}"
        )

        10

      {:error, error} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "calc_pipeline_concurrency/2 failed to fetch_partition_count/2. Setting pipeline concurrency to 10. Error: #{inspect(error)}"
        )

        10

      all_other_errors ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "calc_pipeline_concurrency/2 Failed. Setting pipeline concurrency to 10. Error: #{inspect(all_other_errors)}"
        )

        10
    end
  end
end
