defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Elasticsearch.DocumentBuilders.{EventDocumentBuilder, RiskHistoryDocumentBuilder}
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.System.SystemNotificationContext

  alias Broadway.Message

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @lexicons Application.get_env(:cogynt_workstation_ingest, :core_keys)[:lexicons]
  @defaults %{
    delete_event_ids: nil,
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil
  }

  @doc """
  Based on the crud action value, process_event/1 will create a single
  Event record in the database that is assosciated with the event_definition_id.
  It will also pull all the event_ids and elasticsearch document ids that need to be
  soft_deleted from the database and elasticsearch. The data map is updated with the :event_id,
  :delete_event_ids fields.
  """
  def process_event(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "process_event/1 failed. No message data")
    message
  end

  def process_event(
        %Message{data: %{event_id: event_id, event: %{@crud => action}} = data} = message
      ) do
    case is_nil(event_id) do
      true ->
        {:ok,
         %{
           event_id: new_event_id,
           delete_event_ids: delete_event_ids
         }} =
          case action do
            @delete ->
              delete_event(data)

            _ ->
              create_or_update_event(data)
          end

        data =
          Map.put(data, :event_id, new_event_id)
          |> Map.put(:delete_event_ids, delete_event_ids)
          |> Map.put(:crud_action, action)

        Map.put(message, :data, data)

      false ->
        message
    end
  end

  @doc """
  process_event/1 will create a single Event record in the database
  that is assosciated with the event_definition_id. The data map
  is updated with the :event_id returned from the database.
  """
  def process_event(
        %Message{
          data: %{event_id: event_id, event: event, event_definition: event_definition} = data
        } = message
      ) do
    case is_nil(event_id) do
      true ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            data =
              Map.put(data, :event_id, event_id)
              |> Map.put(:delete_event_ids, @defaults.delete_event_ids)
              |> Map.put(:crud_action, @defaults.crud_action)

            Map.put(message, :data, data)

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "process_event/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "process_event/1 failed"
        end

      false ->
        message
    end
  end

  @doc """
  Takes the field_name and field_value fields from the event and creates a list of event_detail
  maps. Also creates a list of elasticsearch docs. Returns an updated data map with
  the :event_details, :risk_history_doc and :event_docs values.
  """
  def process_event_details_and_elasticsearch_docs(%Message{data: nil} = message) do
    CogyntLogger.warn(
      "#{__MODULE__}",
      "process_event_details_and_elasticsearch_docs/1 failed. No message data"
    )

    message
  end

  def process_event_details_and_elasticsearch_docs(%Message{data: %{event_id: nil}} = message),
    do: message

  def process_event_details_and_elasticsearch_docs(
        %Message{
          data:
            %{
              event: event,
              event_definition: event_definition,
              event_id: event_id,
              event_definition_id: event_definition_id,
              crud_action: action
            } = data
        } = message
      ) do
    event = format_lexicon_data(event)
    core_id = event["id"]
    published_at = event["published_at"]
    confidence = event["_confidence"]
    timestamp = event["_timestamp"]

    # Build event_details
    event_details =
      Enum.reduce(event, [], fn {field_name, field_value}, acc ->
        %{field_type: field_type} =
          Enum.find(event_definition.event_definition_details, %{field_type: nil}, fn %{
                                                                                        field_name:
                                                                                          name
                                                                                      } ->
            name == field_name
          end)

        case is_null_or_empty?(field_value) do
          false ->
            field_value = encode_json(field_value)

            acc ++
              [
                %{
                  event_id: event_id,
                  field_name: field_name,
                  field_type: field_type,
                  field_value: field_value
                }
              ]

          true ->
            acc
        end
      end)

    # Build elasticsearch documents
    elasticsearch_event_doc =
      if action != @delete do
        doc_event_details =
          Enum.filter(event_details, fn event_detail ->
            not is_nil(event_detail.field_type)
          end)

        case EventDocumentBuilder.build_document(
               event_id,
               core_id,
               event_definition.title,
               event_definition_id,
               doc_event_details,
               published_at
             ) do
          {:ok, event_doc} ->
            event_doc

          {:error, _} ->
            @defaults.event_document
        end
      else
        nil
      end

    elasticsearch_risk_history_doc =
      case RiskHistoryDocumentBuilder.build_document(event_id, core_id, confidence, timestamp) do
        {:ok, risk_history_doc} ->
          risk_history_doc

        {:error, :invalid_data} ->
          @defaults.risk_history_document
      end

    data =
      Map.put(data, :event_details, event_details)
      |> Map.put(:event_doc, elasticsearch_event_doc)
      |> Map.put(:risk_history_doc, elasticsearch_risk_history_doc)

    Map.put(message, :data, data)
  end

  @doc """
  process_notifications/1 will stream all notification_settings that are linked to the
  event_definition.id. On each notification_setting returned it will build a notification map.
  Finally it will return a list notification maps. Returns an updated data map with the field
  :notifications storing the list of notification maps.
  """
  def process_notifications(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "process_notifications/1 failed. No message data")
    message
  end

  def process_notifications(%Message{data: %{event_id: nil}} = message), do: message

  def process_notifications(
        %Message{
          data:
            %{
              event: event,
              event_definition: event_definition,
              event_id: event_id,
              delete_event_ids: nil
            } = data
        } = message
      ) do
    case NotificationsContext.process_notifications(%{
           event_definition: event_definition,
           event_id: event_id,
           risk_score: Map.get(event, @risk_score, 0)
         }) do
      {:ok, nil} ->
        message

      {:ok, notifications} ->
        data = Map.put(data, :notifications, notifications)
        Map.put(message, :data, data)
    end
  end

  def process_notifications(%Message{} = message), do: message

  @doc """
  Takes all the fields and executes them in one databse transaction. When
  it finishes with no errors it will update the :event_processed key to have a value of true
  in the data map and return.
  """
  def execute_transaction(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "execute_transaction/1 failed. No message data")
    message
  end

  def execute_transaction(%Message{data: %{event_id: nil}} = message), do: message

  def execute_transaction(
        %Message{
          data: %{
            notifications: notifications,
            event_details: event_details,
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
  processed data and insert it into postgres in bulk
  """
  def execute_batch_transaction(messages) do
    # build transactional data
    default_map = %{
      notifications: [],
      event_details: [],
      event_doc: [],
      risk_history_doc: []
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
            :retry_count
          ])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
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
        EventsContext.insert_all_event_details(bulk_transactional_data.event_details)

      false ->
        transaction_result =
          EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
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
          {:ok,
           %{
             insert_notifications: {_count_created, created_notifications}
           }} ->
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
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp fetch_data_to_delete(%{
         event: event,
         event_definition: event_definition
       }) do
    case event["id"] do
      nil ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "event has CRUD key but missing `id` field. Throwing away record. #{
            inspect(event, pretty: true)
          }"
        )

        {:error, @defaults.delete_event_ids}

      core_id ->
        case EventsContext.get_events_by_core_id(core_id, event_definition.id) do
          [] ->
            {:ok, @defaults.delete_event_ids}

          event_ids ->
            {:ok, event_ids}
        end
    end
  end

  defp delete_event(%{event: event, event_definition: event_definition} = data) do
    # Delete event -> get all data to remove + create a new event
    # append new event to the list of data to remove
    case fetch_data_to_delete(data) do
      {:error, nil} ->
        # will skip all steps in the pipeline ignoring this record
        {:ok,
         %{
           event_id: nil,
           delete_event_ids: @defaults.delete_event_ids
         }}

      {:ok, nil} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: [event_id]
             }}

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "delete_event/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "delete_event/1 failed"
        end

      {:ok, event_ids} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: event_ids ++ [event_id]
             }}

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "delete_event/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "delete_event/1 failed"
        end
    end
  end

  defp create_or_update_event(%{event: event, event_definition: event_definition} = data) do
    # Update event -> get all data to remove + create a new event
    case fetch_data_to_delete(data) do
      {:error, nil} ->
        # will skip all steps in the pipeline ignoring this record
        {:ok,
         %{
           event_id: nil,
           delete_event_ids: @defaults.delete_event_ids
         }}

      {:ok, event_ids} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: event_ids
             }}

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "create_or_update_event/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "create_or_update_event/1 failed"
        end
    end
  end

  defp format_lexicon_data(event) do
    case Map.get(event, @lexicons) do
      nil ->
        event

      lexicon_val ->
        try do
          Map.put(event, @lexicons, List.flatten(lexicon_val))
        rescue
          _ ->
            CogyntLogger.error("#{__MODULE__}", "Lexicon value incorrect format #{lexicon_val}")
            Map.delete(event, @lexicons)
        end
    end
  end

  defp encode_json(value) do
    case String.valid?(value) do
      true ->
        value

      false ->
        Jason.encode!(value)
    end
  end

  defp is_null_or_empty?(enumerable) when is_list(enumerable) do
    Enum.empty?(enumerable)
  end

  defp is_null_or_empty?(binary) do
    is_nil(binary) or binary == ""
  end
end
