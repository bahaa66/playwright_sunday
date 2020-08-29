defmodule CogyntWorkstationIngest.Broadway.LinkEventPipeline do
  @moduledoc """
  Broadway pipeline module for the LinkEventPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Broadway.{Producer, LinkEventProcessor, EventProcessor}

  @pipeline_name :BroadwayLinkEventPipeline

  def start_link(_args) do
    Broadway.start_link(__MODULE__,
      name: @pipeline_name,
      producer: [
        module: {Producer, []},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []}
        # rate_limiting: [
        #   allowed_messages: Config.producer_allowed_messages(),
        #   interval: Config.producer_rate_limit_interval()
        # ]
      ],
      processors: [
        default: [
          concurrency: Config.link_event_processor_stages(),
          max_demand: Config.link_event_processor_max_demand(),
          min_demand: Config.link_event_processor_min_demand()
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
  def transform(payload, _opts) do
    case Jason.decode(payload) do
      {:ok, event} ->
        %Message{
          data: keys_to_atoms(event),
          acknowledger: {__MODULE__, :ack_id, :ack_data}
        }

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode payload. Error: #{inspect(error, pretty: true)}"
        )

        %Message{
          data: nil,
          acknowledger: {__MODULE__, :ack_id, :ack_data}
        }
    end
  end

  @doc """
  Acknowledge callback. Will get all success or failed messages from
  the pipeline.
  """
  def ack(:ack_id, successful, _failed) do
    Enum.each(successful, fn %Broadway.Message{data: %{event_definition_id: event_definition_id}} ->
      {:ok, tmc} = Redis.hash_get("b:#{event_definition_id}", "tmc")
      {:ok, tmp} = Redis.hash_increment_by("b:#{event_definition_id}", "tmp", 1)

      if tmp >= String.to_integer(tmc) do
        finished_processing(event_definition_id)
      end
    end)
  end

  @doc """
  Callback for handling any failed messages in the LinkEventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _args) do
    CogyntLogger.error("#{__MODULE__}", "Messages failed. #{inspect(messages, pretty: true)}")
    Producer.enqueue_failed_messages(messages, @pipeline_name)
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
  def handle_message(_processor, %Message{data: data} = message, _context) do
    data
    |> EventProcessor.fetch_event_definition()
    |> EventProcessor.process_event()
    |> EventProcessor.process_event_details_and_elasticsearch_docs()
    |> EventProcessor.process_notifications()
    |> LinkEventProcessor.validate_link_event()
    |> LinkEventProcessor.process_entities()
    |> LinkEventProcessor.execute_transaction()

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
