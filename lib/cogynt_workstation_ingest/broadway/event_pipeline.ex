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
  alias CogyntWorkstationIngest.Broadway.{EventProcessor, LinkEventProcessor}

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
               session_timeout_seconds: 10
             ],
             client_config: [
               connect_timeout: 10000
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
          Redis.hash_increment_by("emi:#{event_definition_id}", "tmc", 1)

          Map.put(message, :data, %{
            event: decoded_data,
            event_definition_id: event_definition_id,
            event_id: nil,
            retry_count: 0
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
            Map.put(message, :data, data)
            |> Map.drop([:status, :acknowledger])

          acc ++ [message]
        else
          acc
        end
      end)

    if is_nil(event_definition_id) do
      Redis.list_append_pipeline("fem:", failed_messages)
    else
      Redis.list_append_pipeline("fem:#{event_definition_id}", failed_messages)
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
  def handle_message(
        _processor,
        %Message{data: %{event_definition_id: event_definition_id}} = message,
        _context
      ) do
    message = EventProcessor.fetch_event_definition(message)

    case message do
      %Message{data: %{event_definition: %{event_type: :linkage}}} ->
        message
        |> EventProcessor.process_event()
        |> EventProcessor.process_event_details_and_elasticsearch_docs()
        |> EventProcessor.process_notifications()
        |> LinkEventProcessor.validate_link_event()
        |> LinkEventProcessor.process_entities()
        |> LinkEventProcessor.execute_transaction()

      _ ->
        message
        |> EventProcessor.process_event()
        |> EventProcessor.process_event_details_and_elasticsearch_docs()
        |> EventProcessor.process_notifications()
        |> EventProcessor.execute_transaction()
    end

    {:ok, tmc} = Redis.hash_get("emi:#{event_definition_id}", "tmc")
    {:ok, tmp} = Redis.hash_increment_by("emi:#{event_definition_id}", "tmp", 1)

    if tmp >= String.to_integer(tmc) do
      finished_processing(event_definition_id)
    end

    message
  end

  @doc false
  def event_pipeline_running?(event_definition_id) do
    consumer_group_id = ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id)
    child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

    case is_nil(child_pid) do
      true ->
        false

      false ->
        true
    end
  end

  @doc false
  def event_pipeline_finished_processing?(event_definition_id) do
    consumer_group_id = ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id)

    if consumer_group_id == "" do
      true
    else
      case Redis.key_exists?("emi:#{consumer_group_id}") do
        {:ok, false} ->
          true

        {:ok, true} ->
          {:ok, tmc} = Redis.hash_get("emi:#{consumer_group_id}", "tmc")
          {:ok, tmp} = Redis.hash_get("emi:#{consumer_group_id}", "tmp")

          String.to_integer(tmp) >= String.to_integer(tmc)
      end
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp finished_processing(event_definition_id) do
    {:ok,
     %{
       status: status,
       topic: topic,
       backfill_notifications: backfill_notifications,
       update_notifications: update_notifications,
       delete_notifications: delete_notifications
     }} = ConsumerStateManager.get_consumer_state(event_definition_id)

    # If a backfill_notifications, update_notifications or delete_notifications task is triggered
    # while the pipeline is ingesting or processing data. The consumer state manager will update
    # its status to the corresponding task status. Once the pipeline finishes processing it will
    # check here if any of those status were set. If so it will trigger the tasks for all the Ids
    # stored in its corresponding consumer_status key.
    cond do
      status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        Enum.each(backfill_notifications, fn notification_setting_id ->
          ConsumerStateManager.manage_request(%{
            backfill_notifications: notification_setting_id
          })
        end)

      status ==
          ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
        Enum.each(update_notifications, fn notification_setting_id ->
          ConsumerStateManager.manage_request(%{
            update_notifications: notification_setting_id
          })
        end)

      status ==
          ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
        Enum.each(delete_notifications, fn notification_setting_id ->
          ConsumerStateManager.manage_request(%{
            delete_notifications: notification_setting_id
          })
        end)

      status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
        ConsumerStateManager.upsert_consumer_state(event_definition_id,
          topic: topic,
          status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
        )

      true ->
        nil
    end

    Redis.publish_async("event_definitions_subscription", %{updated: event_definition_id})

    CogyntLogger.info(
      "#{__MODULE__}",
      "EventPipeline finished processing messages for EventDefinitionId: #{event_definition_id}"
    )
  end
end
