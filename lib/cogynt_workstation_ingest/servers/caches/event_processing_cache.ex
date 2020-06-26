defmodule CogyntWorkstationIngest.Servers.Caches.EventProcessingCache do
  @moduledoc """
  Keeps track of if an Event_Definition has a consumer in a processing state true
  or false.
  """
  use GenServer
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.ConsumerStateManager

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def update_event_processing_status(event_definition_id, processing)
      when is_boolean(processing) do
    GenServer.cast(__MODULE__, {:update, event_definition_id, processing})
  end

  def remove_event_processing_status(event_definition_id) do
    GenServer.cast(__MODULE__, {:remove, event_definition_id})
  end

  def list_event_processing_status() do
    GenServer.call(__MODULE__, {:list})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:update, event_definition_id, processing}, state) do
    event_is_processing = Map.get(state, event_definition_id)

    new_state =
      if processing == event_is_processing do
        state
      else
        case event_is_processing do
          nil ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Creating Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)

          true ->
            event_finished_processing(event_definition_id)

            CogyntLogger.info(
              "#{__MODULE__}",
              "Changing Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)

          _ ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Changing Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)
        end
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:delayed_finished_processing, event_definition_id, processing}, state) do
    event_is_processing = Map.get(state, event_definition_id)

    CogyntLogger.info(
      "#{__MODULE__}",
      "event_is_processing #{event_is_processing}"
    )

    new_state =
      if processing == event_is_processing or is_nil(event_is_processing) do
        state
      else
        case event_is_processing do
          nil ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Creating Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)

          true ->
            event_finished_processing(event_definition_id)

            CogyntLogger.info(
              "#{__MODULE__}",
              "Changing Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)

          _ ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Changing Processing Status for event_definition_id: #{event_definition_id}. Processing: #{
                processing
              }"
            )

            Map.put(state, event_definition_id, processing)
        end
      end

    {:noreply, new_state}
  end

  @impl true
  def handle_cast({:remove, event_definition_id}, state) do
    new_state = Map.delete(state, event_definition_id)

    CogyntLogger.info(
      "#{__MODULE__}",
      "Removed Processing Status for event_definition_id: #{event_definition_id}"
    )

    {:noreply, new_state}
  end

  @impl true
  def handle_call({:list}, _from, state) do
    {:reply, state, state}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp event_finished_processing(event_definition_id) do
    {:ok, %{status: status, topic: topic, nsid: nsid}} =
      ConsumerStateManager.get_consumer_state(event_definition_id)

    cond do
      status ==
          ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Triggering Backfill notifications task: #{inspect(nsid)}"
        )

        Enum.each(nsid, fn id ->
          ConsumerStateManager.manage_request(%{
            backfill_notifications: id
          })
        end)

      status ==
          ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Triggering Update notifications task: #{inspect(nsid)}"
        )

        Enum.each(nsid, fn id ->
          ConsumerStateManager.manage_request(%{
            update_notification_setting: id
          })
        end)

      status == ConsumerStatusTypeEnum.status()[:paused_and_processing] ->
        ConsumerStateManager.upsert_consumer_state(event_definition_id,
          topic: topic,
          status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
        )

        CogyntClient.publish_consumer_status(
          event_definition_id,
          topic,
          ConsumerStatusTypeEnum.status()[:paused_and_finished]
        )

      true ->
        nil
    end

    CogyntClient.publish_event_definition_ids([event_definition_id])

    CogyntLogger.info(
      "#{__MODULE__}",
      "Finished processing all messages for EventDefinitionId: #{event_definition_id}"
    )
  end
end
