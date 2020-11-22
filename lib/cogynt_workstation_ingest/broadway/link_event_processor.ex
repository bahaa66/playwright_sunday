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
  @defaults %{
    delete_event_ids: nil,
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil
  }

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "validate_link_event/1 failed. No message data")
    message
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
  def process_entities(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "process_entities/1 failed. No message data")
    message
  end

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
  def execute_transaction(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "execute_transaction/1 failed. No message data")
    message
  end

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
            event_doc: event_index_document,
            risk_history_doc: risk_history_index_document,
            crud_action: action,
            event_id: event_id
          }
        } = message
      ) do
    # elasticsearch updates
    if !(delete_event_ids == @defaults.delete_event_ids) do
      {:ok, _} =
        Elasticsearch.bulk_delete_document(
          Config.event_index_alias(),
          delete_event_ids
        )
    end

    if !(event_index_document == @defaults.event_document) do
      {:ok, _} =
        Elasticsearch.upsert_document(
          Config.event_index_alias(),
          event_index_document.id,
          event_index_document
        )
    end

    if !(risk_history_index_document == @defaults.risk_history_document) do
      {:ok, _} =
        Elasticsearch.upsert_document(
          Config.risk_history_index_alias(),
          risk_history_index_document.id,
          risk_history_index_document
        )
    end

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
            event_doc: event_index_document,
            risk_history_doc: risk_history_index_document,
            crud_action: action,
            event_id: event_id
          }
        } = message
      ) do
    # elasticsearch updates
    if !(delete_event_ids == @defaults.delete_event_ids) do
      {:ok, _} =
        Elasticsearch.bulk_delete_document(
          Config.event_index_alias(),
          delete_event_ids
        )
    end

    if !(event_index_document == @defaults.event_document) do
      {:ok, _} =
        Elasticsearch.upsert_document(
          Config.event_index_alias(),
          event_index_document.id,
          event_index_document
        )
    end

    if !(risk_history_index_document == @defaults.risk_history_document) do
      {:ok, _} =
        Elasticsearch.upsert_document(
          Config.risk_history_index_alias(),
          risk_history_index_document.id,
          risk_history_index_document
        )
    end

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

  @doc """
  For datasets that can be processed in bulk this will aggregate all of the
  processed data and insert it into postgresql in bulk
  """
  def execute_batch_transaction(messages) do
    # build transactional data
    default_map = %{
      notifications: [],
      event_details: [],
      event_doc: [],
      risk_history_doc: [],
      link_events: []
    }

    bulk_transactional_data =
      Enum.reduce(messages, default_map, fn data, acc ->
        data =
          Map.drop(data, [
            :crud_action,
            :delete_event_ids,
            :event,
            :event_definition,
            :event_definition_id,
            :event_id,
            :retry_count,
            :validated
          ])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :link_events ->
              v1 ++ v2

            :event_doc ->
              if v2 == @defaults.event_document do
                v1
              else
                v1 ++ [v2]
              end

            :risk_history_doc ->
              if v2 == @defaults.risk_history_document do
                v1
              else
                v1 ++ [v2]
              end

            :notifications ->
              if v2 == @defaults.notifications do
                v1
              else
                v1 ++ v2
              end

            _ ->
              v1 ++ v2
          end
        end)
      end)

    # elasticsearch updates
    if !Enum.empty?(bulk_transactional_data.event_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.event_index_alias(),
          bulk_transactional_data.event_doc
        )
    end

    if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.risk_history_index_alias(),
          bulk_transactional_data.risk_history_doc
        )
    end

    # Run database transaction
    case Enum.empty?(bulk_transactional_data.notifications) do
      true ->
        transaction_result =
          EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
          |> EventsContext.insert_all_event_links_multi(bulk_transactional_data.link_events)
          |> EventsContext.run_multi_transaction()

        case transaction_result do
          {:ok, _} ->
            nil

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_batch_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "execute_batch_transaction/1 failed"
        end

      false ->
        transaction_result =
          EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
          |> EventsContext.insert_all_event_links_multi(bulk_transactional_data.link_events)
          |> NotificationsContext.insert_all_notifications_multi(
            bulk_transactional_data.notifications,
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
          |> EventsContext.run_multi_transaction()

        case transaction_result do
          {:ok, %{insert_notifications: {_count_created, created_notifications}}} ->
            SystemNotificationContext.bulk_insert_system_notifications(created_notifications)

          {:ok, _} ->
            nil

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_batch_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "execute_batch_transaction/1 failed"
        end
    end

    messages
  end
end
