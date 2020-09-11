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
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.EventProcessor

  @defaults %{
    event_id: nil,
    retry_count: 0
  }

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
        transformer:
          {__MODULE__, :transform, [group_id: group_id, event_definition_id: event_definition_id]}
      ],
      processors: [
        default: [
          concurrency: Config.event_processor_stages()
        ]
      ]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, opts) do
    IO.inspect(message, label: "Event Message data")
    group_id = Keyword.get(opts, :group_id, 1)
    event_definition_id = Keyword.get(opts, :event_definition_id, nil)

    case Jason.decode(encoded_data) do
      {:ok, decoded_data} ->
        # Incr the total message count that has been consumed from kafka
        Redis.hash_increment_by("emi:#{group_id}", "tmc", 1)

        Map.put(message, :data, %{
          event: keys_to_atoms(decoded_data),
          event_definition_id: event_definition_id,
          event_id: @defaults.event_id
        })
        |> Map.put(:acknowledger, {__MODULE__, group_id, :ack_data})

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode Kafka message. Error: #{inspect(error)}"
        )

        Map.put(message, :data, nil)
        |> Map.put(:acknowledger, {__MODULE__, group_id, :ack_data})
    end
  end

  @doc """
  Acknowledge callback. Will get all success or failed messages from
  the pipeline.
  """
  def ack(group_id, successful, _failed) do
    Enum.each(successful, fn %Broadway.Message{data: %{event_definition_id: event_definition_id}} ->
      {:ok, tmc} = Redis.hash_get("emi:#{group_id}", "tmc")
      {:ok, tmp} = Redis.hash_increment_by("emi:#{group_id}", "tmp", 1)

      if tmp >= String.to_integer(tmc) do
        finished_processing(event_definition_id)
      end
    end)
  end

  @doc """
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _args) do
    CogyntLogger.error("#{__MODULE__}", "Messages failed. #{inspect(messages)}")
    # TODO: handle failed messages
    # Producer.enqueue_failed_messages(messages, @pipeline_name)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_event/1, process_event_details_and_elasticsearch_docs/1,
  process_notifications/1 and execute_transaction/1.
  """
  @impl true
  def handle_message(_processor, message, _context) do
    message
    |> EventProcessor.fetch_event_definition()
    |> EventProcessor.process_event()
    |> EventProcessor.process_event_details_and_elasticsearch_docs()
    |> EventProcessor.process_notifications()
    |> EventProcessor.execute_transaction()

    message
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp keys_to_atoms(string_key_map) when is_map(string_key_map) do
    for {key, val} <- string_key_map, into: %{} do
      {String.to_atom(key), val}
    end
  end

  defp finished_processing(event_definition_id) do
    {:ok,
     %{
       status: status,
       topic: topic,
       backfill_notifications: backfill_notifications,
       update_notifications: update_notifications,
       delete_notifications: delete_notifications
     }} = ConsumerStateManager.get_consumer_state(event_definition_id)

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
      "Finished processing all messages for EventDefinitionId: #{event_definition_id}"
    )
  end
end
