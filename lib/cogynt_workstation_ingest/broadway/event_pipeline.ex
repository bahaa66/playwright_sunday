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
  alias Kafka.Api.Topic

  def start_link(%{
        group_id: group_id,
        topics: topics,
        hosts: hosts,
        event_definition_hash_id: event_definition_hash_id,
        event_type: event_type
      }) do
    ### --------- TEMP to find out if this has CRUD or not ---------------- ###
    {batchers, context} =
      case use_crud_pipeline?(topics, hosts) do
        {:ok, true} ->
          {[
             crud: [
               batch_size: Config.event_pipeline_batch_size(),
               batch_timeout: 30000,
               concurrency: 10
             ]
           ],
           [
             event_definition_hash_id: event_definition_hash_id,
             event_type: event_type,
             crud: true
           ]}

        {:ok, false} ->
          {[
             default: [
               batch_size: Config.event_pipeline_batch_size(),
               batch_timeout: 30000,
               concurrency: 10
             ]
           ],
           [
             event_definition_hash_id: event_definition_hash_id,
             event_type: event_type,
             crud: false
           ]}

        {:error, _} ->
          {[
             default: [
               batch_size: Config.event_pipeline_batch_size(),
               batch_timeout: 30000,
               concurrency: 10
             ],
             crud: [
               batch_size: Config.event_pipeline_batch_size(),
               batch_timeout: 30000,
               concurrency: 10
             ]
           ],
           [event_definition_hash_id: event_definition_hash_id, event_type: event_type, crud: nil]}
      end

    ### --------- TEMP to find out if this has CRUD or not ---------------- ###

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
               # 3 Mib
               max_bytes: 3_146_576
             ]
           ]},
        concurrency: 1,
        transformer:
          {__MODULE__, :transform,
           [event_definition_hash_id: event_definition_hash_id, event_type: event_type]}
      ],
      processors: [
        default: [
          concurrency: 10
        ]
      ],
      batchers: batchers,
      context: context
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, opts) do
    event_definition_hash_id = Keyword.get(opts, :event_definition_hash_id, nil)
    event_type = Keyword.get(opts, :event_type, "none")

    if is_nil(event_definition_hash_id) do
      CogyntLogger.error(
        "#{__MODULE__}",
        "event_definition_hash_id was not passed to EventPipeline. Passing empty data object to the EventPipeline"
      )

      Map.put(message, :data, nil)
    else
      case Jason.decode(encoded_data) do
        {:ok, decoded_data} ->
          # Incr the total message count that has been consumed from kafka
          incr_total_fetched_message_count(event_definition_hash_id)

          Map.put(message, :data, %{
            event: decoded_data,
            event_definition_hash_id: event_definition_hash_id,
            core_id: decoded_data[Config.id_key()] || Ecto.UUID.generate(),
            pipeline_state: nil,
            retry_count: 0,
            event_type: event_type,
            event_definition:
              EventsContext.get_event_definition(event_definition_hash_id, preload_details: true)
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
  def handle_failed(messages, _context) do
    # event_definition_hash_id = Keyword.get(context, :event_definition_hash_id, nil)

    # incr_total_processed_message_count(event_definition_hash_id, Enum.count(messages))

    # failed_messages =
    #   Enum.reduce(messages, [], fn %Broadway.Message{
    #                                  data:
    #                                    %{
    #                                      event_definition_hash_id: event_definition_hash_id,
    #                                      retry_count: retry_count
    #                                    } = data
    #                                } = message,
    #                                acc ->
    #     if retry_count < Config.failed_messages_max_retry() do
    #       new_retry_count = retry_count + 1

    #       CogyntLogger.warn(
    #         "#{__MODULE__}",
    #         "Retrying Failed EventPipeline Message. EventDefinitionHashId: #{event_definition_hash_id}. Attempt: #{
    #           new_retry_count
    #         }"
    #       )

    #       data = Map.put(data, :retry_count, new_retry_count)

    #       message =
    #         Map.from_struct(message)
    #         |> Map.put(:data, data)
    #         |> Map.drop([:status, :acknowledger])

    #       metadata =
    #         Map.get(message, :metadata)
    #         |> Map.put(:key, "")

    #       message = Map.put(message, :metadata, metadata)

    #       acc ++ [message]
    #     else
    #       acc
    #     end
    #   end)

    # key =
    #   if is_nil(event_definition_hash_id) do
    #     "fem:"
    #   else
    #     "fem:#{event_definition_hash_id}"
    #   end

    # case Redis.list_length(key) do
    #   {:ok, length} when length >= 50_000 ->
    #     CogyntLogger.error(
    #       "#{__MODULE__}",
    #       "Failed Event messages for key #{key} have reached the limit of 50_000 in Redis. Dropping future messages from getting queued"
    #     )

    #   _ ->
    #     Redis.list_append_pipeline(key, failed_messages)
    #     # 30 min TTL
    #     Redis.key_pexpire(key, 1_800_000)
    # end

    CogyntLogger.warn(
      "#{__MODULE__}",
      "handle_failed/2 #{Enum.count(messages)} Failed for EventPipeline. Check logs to for error."
    )

    messages
  end

  # handle_message/3 method for when we know the type of data for this pipeline will have
  # `crud` data and only needs the `crud` batcher
  @impl true
  def handle_message(
        _processor_name,
        message,
        event_definition_hash_id: _event_definition_hash_id,
        event_type: _,
        crud: true
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    data =
      case message.data.pipeline_state do
        :process_event ->
          message.data
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :validate_link_event ->
          message.data
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :process_entities ->
          message.data
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :process_event_details_and_elasticsearch_docs ->
          message.data
          |> EventProcessor.process_notifications()

        _ ->
          message.data
          |> EventProcessor.process_event_history()
          |> EventProcessor.process_event()
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()
      end

    # Execute telemtry for metrics
    :telemetry.execute(
      [:broadway, :event_processor_all_crud_processing_stages],
      %{duration: System.monotonic_time() - start},
      telemetry_metadata
    )

    Map.put(message, :data, data)
    # |> Message.put_batch_key(event_definition_hash_id)
    |> Message.put_batcher(:crud)
  end

  # handle_message/3 method for when we know the type of data for this pipeline will not have
  # `crud` data and only needs the `default` batcher
  @impl true
  def handle_message(
        _processor_name,
        message,
        event_definition_hash_id: _event_definition_hash_id,
        event_type: _,
        crud: false
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    data =
      case message.data.pipeline_state do
        :process_event ->
          message.data
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :validate_link_event ->
          message.data
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :process_entities ->
          message.data
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()

        :process_event_details_and_elasticsearch_docs ->
          message.data
          |> EventProcessor.process_notifications()

        _ ->
          message.data
          |> EventProcessor.process_event()
          |> LinkEventProcessor.validate_link_event()
          |> LinkEventProcessor.process_entities()
          |> EventProcessor.process_elasticsearch_documents()
          |> EventProcessor.process_notifications()
      end

    # Execute telemtry for metrics
    :telemetry.execute(
      [:broadway, :event_processor_all_processing_stages],
      %{duration: System.monotonic_time() - start},
      telemetry_metadata
    )

    Map.put(message, :data, data)
    # |> Message.put_batch_key(event_definition_hash_id)
    |> Message.put_batcher(:default)
  end

  # handle_message/3 method for when we have to start a pipeline with both the `crud` and `default` batchers.
  # A scenario when we are unsure about the type of data that exists on the topic when the pipeline is started
  @impl true
  def handle_message(
        _processor_name,
        message,
        event_definition_hash_id: _event_definition_hash_id,
        event_type: _,
        crud: nil
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    message
    |> case do
      %Message{data: %{event: event}} ->
        case Map.get(event, Config.crud_key(), nil) do
          nil ->
            data =
              case message.data.pipeline_state do
                :process_event ->
                  message.data
                  |> LinkEventProcessor.validate_link_event()
                  |> LinkEventProcessor.process_entities()
                  |> EventProcessor.process_elasticsearch_documents()
                  |> EventProcessor.process_notifications()

                :validate_link_event ->
                  message.data
                  |> LinkEventProcessor.process_entities()
                  |> EventProcessor.process_elasticsearch_documents()
                  |> EventProcessor.process_notifications()

                :process_entities ->
                  message.data
                  |> EventProcessor.process_elasticsearch_documents()
                  |> EventProcessor.process_notifications()

                :process_event_details_and_elasticsearch_docs ->
                  message.data
                  |> EventProcessor.process_notifications()

                _ ->
                  message.data
                  |> EventProcessor.process_event()
                  |> LinkEventProcessor.validate_link_event()
                  |> LinkEventProcessor.process_entities()
                  |> EventProcessor.process_elasticsearch_documents()
                  |> EventProcessor.process_notifications()
              end

            # Execute telemtry for metrics
            :telemetry.execute(
              [:broadway, :event_processor_all_processing_stages],
              %{duration: System.monotonic_time() - start},
              telemetry_metadata
            )

            Map.put(message, :data, data)
            # |> Message.put_batch_key(event_definition_hash_id)
            |> Message.put_batcher(:default)

          _ ->
            data =
              message.data
              |> EventProcessor.process_event_history()

            Map.put(message, :data, data)
            # |> Message.put_batch_key(event_definition_hash_id)
            |> Message.put_batcher(:crud)
        end
    end
  end

  @impl true
  def handle_batch(:default, messages, _batch_info, context) do
    event_definition_hash_id = Keyword.get(context, :event_definition_hash_id, nil)
    event_type = Keyword.get(context, :event_type, nil)

    messages
    |> Enum.map(fn message -> message.data end)
    |> EventProcessor.execute_batch_transaction(event_type)

    incr_total_processed_message_count(event_definition_hash_id, Enum.count(messages))
    messages
  end

  @impl true
  def handle_batch(:crud, messages, _batch_info, context) do
    event_definition_hash_id = Keyword.get(context, :event_definition_hash_id, nil)
    event_type = Keyword.get(context, :event_type, nil)

    IO.puts("------------------------------------------------")
    IO.inspect(Enum.count(messages), label: "CRUD BATCH COUNT")

    crud_bulk_data =
      messages
      |> Enum.group_by(fn message -> message.data.core_id end)
      |> Enum.reduce([], fn {_core_id, core_id_records}, acc ->
        # We only need to process the last action that occurred for the
        # core_id within the batch of events that were sent to handle_batch
        # ex: create, update, update, update, delete, create (only need the last create)
        last_crud_action_message = List.last(core_id_records)

        acc ++ [last_crud_action_message.data]
      end)

    EventProcessor.execute_batch_transaction(crud_bulk_data, event_type)

    incr_total_processed_message_count(event_definition_hash_id, Enum.count(messages))
    messages
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

  defp incr_total_fetched_message_count(event_definition_hash_id) do
    Redis.hash_increment_by("emi:#{event_definition_hash_id}", "tmc", 1)
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

  defp use_crud_pipeline?(topics, hosts) do
    topic = List.first(topics)

    {_status, %{min_offset: earliest_offset, max_offset: latest_offset}} =
      Topic.get_partition_offset_meta(topic, 0, hosts)

    total = latest_offset - earliest_offset
    last_offset = latest_offset - 1

    cond do
      total > 0 ->
        case Topic.fetch_message(topic, 0, last_offset, hosts) do
          {:ok, %{payload: payload}} ->
            if payload != nil and is_binary(payload) and byte_size(payload) > 0 do
              case Jason.decode(payload, keys: :atoms) do
                {:ok, decoded_payload} ->
                  if Map.has_key?(decoded_payload, :COG_crud) do
                    {:ok, true}
                  else
                    {:ok, false}
                  end

                _ ->
                  {:error, :failed}
              end
            else
              {:error, :failed}
            end

          _ ->
            {:error, :failed}
        end

      true ->
        {:error, :failed}
    end
  end
end
