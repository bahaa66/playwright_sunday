defmodule CogyntWorkstationIngest.Servers.PubSub.IngestPubSub do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.DynamicTaskSupervisor

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
    case Jason.decode(json_payload, keys: :atoms) do
      {:ok, %{start_consumer: event_definition} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        {:ok, created_at, _} = DateTime.from_iso8601(event_definition.created_at)
        {:ok, updated_at, _} = DateTime.from_iso8601(event_definition.updated_at)

        deleted_at =
          case is_nil(event_definition.deleted_at) do
            false ->
              {:ok, deleted_at, _} = DateTime.from_iso8601(event_definition.deleted_at)
              deleted_at

            true ->
              nil
          end

        started_at =
          case is_nil(event_definition.started_at) do
            false ->
              {:ok, started_at, _} = DateTime.from_iso8601(event_definition.started_at)
              started_at

            true ->
              nil
          end

        event_definition =
          event_definition
          |> Map.put(:deleted_at, deleted_at)
          |> Map.put(:created_at, created_at)
          |> Map.put(:started_at, started_at)
          |> Map.put(:updated_at, updated_at)

        ConsumerStateManager.manage_request(%{start_consumer: event_definition})

      {:ok, %{stop_consumer: event_definition} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{stop_consumer: event_definition})

      {:ok, %{backfill_notifications: notification_setting_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{backfill_notifications: notification_setting_id})

      {:ok, %{update_notifications: notification_setting_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{
          update_notifications: notification_setting_id
        })

      {:ok, %{delete_notifications: notification_setting_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{
          delete_notifications: notification_setting_id
        })

      {:ok, %{delete_event_definition_events: event_definition_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{delete_event_definition_events: event_definition_id})

      {:ok,
       %{
         dev_delete: %{
           drilldown: %{
             reset_drilldown: reset_drilldown,
             delete_drilldown_topics: delete_drilldown_topics
           },
           deployment: reset_deployment,
           event_definitions: %{
             event_definition_ids: event_definition_ids,
             delete_topics: delete_topics
           }
         }
       } = request} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        try do
          if reset_deployment do
            DynamicTaskSupervisor.start_child(%{delete_deployment_data: true})
          else
            if reset_drilldown do
              DynamicTaskSupervisor.start_child(%{
                delete_drilldown_data: delete_drilldown_topics
              })
            end

            if length(event_definition_ids) > 0 do
              DynamicTaskSupervisor.start_child(%{
                delete_event_definitions_and_topics: %{
                  event_definition_ids: event_definition_ids,
                  hard_delete: false,
                  delete_topics: delete_topics
                }
              })
            end
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
