defmodule CogyntWorkstationIngest.Servers.PubSub.IngestPubSub do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(pubsub) do
    GenServer.start_link(__MODULE__, pubsub, name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(pubsub) do
    {:ok, ref} = Redis.subscribe(pubsub, "ingest_channel", self())
    {:ok, %{ref: ref}}
  end

  @impl true
  def handle_info({:redix_pubsub, _pubsub, _ref, :subscribed, %{channel: channel}}, state) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Successfully subscribed to channel: #{inspect(channel, pretty: true)}"
    )

    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub, _ref, :message, %{channel: channel, payload: json_payload}},
        state
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Channel: #{inspect(channel)}, Received message: #{inspect(json_payload, pretty: true)}"
    )

    case Jason.decode(json_payload, keys: :atoms) do
      {:ok, %{start_consumer: event_definition}} ->
        ConsumerStateManager.manage_request(%{start_consumer: event_definition})

      {:ok, %{stop_consumer: event_definition}} ->
        ConsumerStateManager.manage_request(%{stop_consumer: event_definition.id})

      {:ok, %{backfill_notifications: notification_setting_id}} ->
        ConsumerStateManager.manage_request(%{backfill_notifications: notification_setting_id})

      {:ok, %{update_notification_setting: notification_setting_id}} ->
        ConsumerStateManager.manage_request(%{
          update_notification_setting: notification_setting_id
        })

      {:ok, %{delete_event_definition_events: event_definition_id}} ->
        ConsumerStateManager.manage_request(%{delete_event_definition_events: event_definition_id})

      {:ok,
       %{
         dev_delete: %{
           drilldown: reset_drilldown,
           event_definition_ids: event_definition_ids,
           topics: delete_topics
         }
       }} ->
        try do
          if reset_drilldown do
            TaskSupervisor.start_child(%{delete_drilldown_data: delete_topics})
          end

          if length(event_definition_ids) > 0 do
            TaskSupervisor.start_child(%{
              delete_topic_data: %{
                event_definition_ids: event_definition_ids,
                delete_topics: delete_topics
              }
            })
          end
        rescue
          error ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "dev_delete failed with error: #{inspect(error, pretty: true)}"
            )
        end

      {:ok, _} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Subscription carrying invalid payload. Does not match any command on IngestServer: #{
            inspect(json_payload, pretty: true)
          }"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode subscription payload. Error: #{inspect(error, pretty: true)}"
        )
    end

    {:noreply, state}
  end
end
