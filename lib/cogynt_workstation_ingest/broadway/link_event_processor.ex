defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Broadway.EventProcessor
  alias CogyntWorkstationIngest.System.SystemNotificationContext
  alias CogyntWorkstationIngest.Config

  alias Broadway.Message

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%Message{data: nil}) do
    raise "validate_link_event/1 failed. No message data"
  end

  def validate_link_event(%Message{data: %{event: event} = data} = message) do
    case Map.get(event, @entities) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "link event missing entities field. LinkEvent: #{inspect(event, pretty: true)}"
        )

        data = Map.put(data, :validated, false)
        Map.put(message, :data, data)

      entities ->
        data =
          if Enum.empty?(entities) do
            CogyntLogger.warn(
              "#{__MODULE__}",
              "entity field is empty. Entity: #{inspect(entities, pretty: true)}"
            )

            Map.put(data, :validated, false)
          else
            Map.put(data, :validated, true)
          end

        Map.put(message, :data, data)
    end
  end

  @doc """
  Requires event fields in the data map. process_entities/1 will parse the entities keys value
  and pull out just the "id" fields. Ex: ${"locations" => [1, 2, 3], "accounts" => [5, 6]}. Will
  udpate the data map with a new :link_entities value storing the return value.
  """
  def process_entities(%Message{data: nil}),
    do: raise("process_entities/1 failed. No message data")

  def process_entities(%Message{data: %{validated: false}} = message), do: message
  def process_entities(%Message{data: %{event_id: nil}} = message), do: message

  def process_entities(
        %Message{data: %{event: %{@entities => entities}, event_id: event_id} = data} = message
      ) do
    entity_links =
      Enum.reduce(entities, [], fn {_key, link_object_list}, acc_0 ->
        objects_links =
          Enum.reduce(link_object_list, [], fn link_object, acc_1 ->
            case link_object["id"] do
              nil ->
                CogyntLogger.warn(
                  "#{__MODULE__}",
                  "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
                )

                acc_1

              core_id ->
                acc_1 ++ [%{linkage_event_id: event_id, core_id: core_id}]
            end
          end)

        acc_0 ++ objects_links
      end)

    data = Map.put(data, :link_events, entity_links)
    Map.put(message, :data, data)
  end

  @doc """
  Requires :event_links fields in the data map. Takes all the fields and
  executes them in one databse transaction.
  """
  def execute_transaction(%Message{data: nil}),
    do: raise("execute_transaction/1 failed. No message data")

  def execute_transaction(%Message{data: %{event_id: nil}} = message), do: message

  def execute_transaction(%Message{data: %{validated: false}} = message),
    do: EventProcessor.execute_transaction(message)

  def execute_transaction(
        %Message{
          data: %{
            notifications: notifications,
            event_details: event_details,
            link_events: link_events,
            delete_event_ids: delete_event_ids,
            event_docs: event_doc_data,
            risk_history_doc: risk_history_doc,
            delete_docs: doc_ids,
            crud_action: action,
            event_id: event_id
          }
        } = message
      ) do
    # elasticsearch updates
    update_event_docs(event_doc_data, doc_ids)
    update_risk_history_doc(risk_history_doc)

    transaction_result =
      EventsContext.insert_all_event_details_multi(event_details)
      |> EventsContext.insert_all_event_links_multi(link_events)
      |> NotificationsContext.insert_all_notifications_multi(notifications,
        returning: [
          :event_id,
          :user_id,
          :tag_id,
          :id,
          :title,
          :notification_setting_id,
          :created_at,
          :updated_at,
          :assigned_to
        ]
      )
      |> EventsContext.update_all_events_multi(delete_event_ids)
      |> NotificationsContext.update_all_notifications_multi(%{
        delete_event_ids: delete_event_ids,
        action: action,
        event_id: event_id
      })
      |> EventsContext.update_all_event_links_multi(delete_event_ids)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok,
       %{
         insert_notifications: {_count_created, created_notifications},
         update_notifications: {_count_deleted, updated_notifications}
       }} ->
        SystemNotificationContext.bulk_insert_system_notifications(created_notifications)
        SystemNotificationContext.bulk_update_system_notifications(updated_notifications)

      {:ok, %{insert_notifications: {_count_created, created_notifications}}} ->
        SystemNotificationContext.bulk_insert_system_notifications(created_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "execute_transaction/1 failed"
    end

    message
  end

  def execute_transaction(
        %Message{
          data: %{
            event_details: event_details,
            link_events: link_events,
            delete_event_ids: delete_event_ids,
            event_docs: event_doc_data,
            risk_history_doc: risk_history_doc,
            delete_docs: doc_ids,
            crud_action: action,
            event_id: event_id
          }
        } = message
      ) do
    # elasticsearch updates
    update_event_docs(event_doc_data, doc_ids)
    update_risk_history_doc(risk_history_doc)

    transaction_result =
      EventsContext.insert_all_event_details_multi(event_details)
      |> EventsContext.insert_all_event_links_multi(link_events)
      |> EventsContext.update_all_events_multi(delete_event_ids)
      |> NotificationsContext.update_all_notifications_multi(%{
        delete_event_ids: delete_event_ids,
        action: action,
        event_id: event_id
      })
      |> EventsContext.update_all_event_links_multi(delete_event_ids)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok, %{update_notifications: {_count, updated_notifications}}} ->
        SystemNotificationContext.bulk_update_system_notifications(updated_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "execute_transaction/1 failed"
    end

    message
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp update_event_docs(
         %{event_docs: event_docs, remove_docs_for_update: remove_docs_for_update},
         delete_event_doc_ids
       ) do
    if is_null_or_empty?(delete_event_doc_ids) == false do
      {:ok, _} =
        Elasticsearch.bulk_delete_document(Config.event_index_alias(), delete_event_doc_ids)
    else
      if is_null_or_empty?(remove_docs_for_update) == false do
        {:ok, _} =
          Elasticsearch.bulk_delete_document(Config.event_index_alias(), remove_docs_for_update)
      end

      if is_null_or_empty?(event_docs) == false do
        {:ok, %{"errors" => errors}} =
          Elasticsearch.bulk_upsert_document(
            Config.event_index_alias(),
            event_docs
          )

        if errors do
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Elasticsearch.bulk_upsert_document/3 errors was true"
          )
        end
      end
    end
  end

  defp update_risk_history_doc(risk_history_doc) do
    case !is_nil(risk_history_doc) do
      true ->
        {:ok, _} =
          Elasticsearch.upsert_document(
            Config.risk_history_index_alias(),
            risk_history_doc.id,
            risk_history_doc
          )

      false ->
        :ok
    end
  end

  defp is_null_or_empty?(enumerable) do
    is_nil(enumerable) or Enum.empty?(enumerable)
  end
end
