defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Elasticsearch.DocumentBuilders.{EventDocumentBuilder, RiskHistoryDocumentBuilder}
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.System.SystemNotificationContext
  alias Models.Notifications.Notification

  alias Broadway.Message
  alias Ecto.Multi

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @lexicons Application.get_env(:cogynt_workstation_ingest, :core_keys)[:lexicons]
  @defaults %{
    delete_event_ids: nil,
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil,
    event_id: nil
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
               core_id: event["id"],
               occurred_at: event["_timestamp"]
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

  # Only need to create a notification if there is no previous events linked via its core_id. (crud data)
  # This is because even whenever a notification setting is created in Admin console we first stop the
  # corresponding event_pipeline and backfill all the notifications for all events that may have been ingested
  # and when the pipleine is started again it will start creating notifications via this method again.
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
  For datasets that have $CRUD keys present. This data needs
  more pre processing to be done before it can be processed in
  bulk
  """
  def execute_batch_transaction_for_crud(core_id_data_map) do
    # build transactional data
    default_map = %{
      event_details: [],
      delete_event_ids: [],
      notifications: [],
      event_id: nil,
      event: nil,
      event_definition_id: nil,
      crud_action: nil,
      event_doc: [],
      risk_history_doc: []
    }

    # First iterrate through each key in the map and for each event map that is stored for
    # its value, loop through and merge the maps. Result should be a new map with keys being
    # the core_id and the values being one map with all the combined values
    core_id_data_map =
      Enum.reduce(core_id_data_map, Map.new(), fn {key, values}, acc_0 ->
        new_data =
          Enum.reduce(values, default_map, fn %Broadway.Message{data: data}, acc_1 ->
            data =
              Map.drop(data, [
                :event_definition,
                :retry_count
              ])

            Map.merge(acc_1, data, fn k, v1, v2 ->
              case k do
                :event_details ->
                  v1 ++ v2

                :delete_event_ids ->
                  if v2 == @defaults.delete_event_ids or Enum.empty?(v2) do
                    v1
                  else
                    Enum.uniq(v1 ++ v2)
                  end

                :notifications ->
                  if v2 == @defaults.notifications or Enum.empty?(v2) do
                    v1
                  else
                    v1 ++ v2
                  end

                :event_definition_id ->
                  v2

                :event ->
                  v2

                :event_id ->
                  v2

                :crud_action ->
                  v2

                :event_doc ->
                  if v2 == @defaults.event_document or Enum.empty?(v2) do
                    v1
                  else
                    v1 ++ [v2]
                  end

                :risk_history_doc ->
                  if v2 == @defaults.risk_history_document or Enum.empty?(v2) do
                    v1
                  else
                    if Enum.empty?(v1) do
                      v1 ++ [v2]
                    else
                      temp_doc = List.first(v1)
                      new_risk = temp_doc.risk_history ++ v2.risk_history

                      [
                        %{
                          id: v2.id,
                          risk_history: new_risk
                        }
                      ]
                    end
                  end

                _ ->
                  v2
              end
            end)
          end)

        Map.put(acc_0, key, new_data)
      end)

    # Second itterate through the new map now merging together each map stored for each key
    default_map = %{
      event_doc: [],
      risk_history_doc: [],
      delete_event_ids: [],
      event_details: [],
      notifications: []
    }

    bulk_transactional_data =
      Enum.reduce(core_id_data_map, default_map, fn {_key, data}, acc ->
        data = Map.drop(data, [:event_id])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :delete_event_ids ->
              if v2 == @defaults.delete_event_ids or Enum.empty?(v2) do
                v1
              else
                Enum.uniq(v1 ++ v2)
              end

            :notifications ->
              if v2 == @defaults.notifications or Enum.empty?(v2) do
                v1
              else
                v1 ++ v2
              end

            :event_doc ->
              if v2 == @defaults.event_document or Enum.empty?(v2) do
                v1
              else
                v1 ++ v2
              end

            :risk_history_doc ->
              if v2 == @defaults.risk_history_document or Enum.empty?(v2) do
                v1
              else
                v1 ++ v2
              end

            _ ->
              v2
          end
        end)
      end)

    IO.inspect(bulk_transactional_data.notifications, label: "Created Notifications")

    # Third itterate over the map that still holds the key = core_id and values = combined event maps
    # and build a Multi transactional object for each Crud key for the method `update_all_notifications`
    multi = Multi.new()

    multi =
      Enum.reduce(core_id_data_map, multi, fn {_key, data}, acc ->
        delete_event_ids = Map.get(data, :delete_event_ids)
        crud_action = Map.get(data, :crud_action)
        event = Map.get(data, :event, %{})
        event_risk_score = event["_confidence"]
        event_definition_id = Map.get(data, :event_definition_id)
        event_id = Map.get(data, :event_id)

        case is_nil(delete_event_ids) or Enum.empty?(delete_event_ids) do
          true ->
            multi

          false ->
            start = Time.utc_now()

            # First find fetch all the Notification_settings for the EventDefinitionId and
            # filter out all invalid notification_settings. Only notification_settings that match the current
            # events criteria
            valid_notification_settings =
              NotificationsContext.query_notification_settings(%{
                filter: %{event_definition_id: event_definition_id, deleted_at: nil}
              })
              |> Enum.filter(fn notification_setting ->
                NotificationsContext.in_risk_range?(
                  event_risk_score,
                  notification_setting.risk_range
                )
              end)

            IO.inspect(valid_notification_settings, label: "VALID NOTIFICATION SETTINGS:")

            # Second fetch all the Notifications that were created against the deleted_event_ids
            # and create a new list of notifications to either be updated or deleted based on the
            # list of valid_notification_settings
            notifications =
              NotificationsContext.query_notifications(%{
                filter: %{event_ids: delete_event_ids},
                select: Notification.__schema__(:fields)
              })

            IO.inspect(notifications, label: "Notifications From Query:")

            notifications =
              Enum.reduce(notifications, [], fn notification, acc ->
                ns =
                  Enum.find(valid_notification_settings, fn notification_setting ->
                    notification.notification_setting_id == notification_setting.id
                  end)

                IO.inspect(ns, label: "Notification matched Notification Setting")

                if is_nil(ns) do
                  acc ++
                    [
                      NotificationsContext.generate_notification_struct(%{
                        id: notification.id,
                        title: notification.title,
                        # description: notification.description,
                        user_id: notification.user_id,
                        archived_at: notification.archived_at,
                        priority: notification.priority,
                        assigned_to: notification.assigned_to,
                        dismissed_at: notification.dismissed_at,
                        deleted_at: DateTime.truncate(DateTime.utc_now(), :second),
                        event_id: notification.event_id,
                        notification_setting_id: notification.notification_setting_id,
                        tag_id: notification.tag_id,
                        created_at: notification.created_at,
                        updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                      })
                    ]
                else
                  deleted_at =
                    if crud_action == @delete do
                      DateTime.truncate(DateTime.utc_now(), :second)
                    else
                      nil
                    end

                  acc ++
                    [
                      NotificationsContext.generate_notification_struct(%{
                        id: notification.id,
                        title: ns.title,
                        # description: notification.description,
                        user_id: ns.user_id,
                        archived_at: notification.archived_at,
                        priority: notification.priority,
                        assigned_to: ns.assigned_to,
                        dismissed_at: notification.dismissed_at,
                        deleted_at: deleted_at,
                        event_id: event_id,
                        notification_setting_id: ns.id,
                        tag_id: ns.tag_id,
                        created_at: notification.created_at,
                        updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                      })
                    ]
                end
              end)

            finish = Time.utc_now()
            diff = Time.diff(finish, start, :millisecond)
            IO.puts("DURATION OF NEW NOTIFICATION LOGIC: #{diff}, PID: #{inspect(self())}")

            NotificationsContext.upsert_all_notifications_multi(
              acc,
              String.to_atom("upsert_notifications" <> ":" <> "#{event_id}"),
              notifications,
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
        end
      end)

    # elasticsearch updates
    # TODO: instead of creating all the documents and then in the next
    # step removing a subset of the documents you just created. Add a step
    # to just filter out those documents from the event_doc list so they are
    # never created
    if !Enum.empty?(bulk_transactional_data.event_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.event_index_alias(),
          bulk_transactional_data.event_doc
        )
    end

    if !Enum.empty?(bulk_transactional_data.delete_event_ids) do
      {:ok, _result} =
        Elasticsearch.bulk_delete_document(
          Config.event_index_alias(),
          bulk_transactional_data.delete_event_ids
        )
    end

    if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.risk_history_index_alias(),
          bulk_transactional_data.risk_history_doc
        )
    end

    # Build database transaction
    case Enum.empty?(bulk_transactional_data.notifications) do
      true ->
        transaction_result =
          EventsContext.insert_all_event_details_multi(
            multi,
            bulk_transactional_data.event_details
          )
          |> EventsContext.update_all_events_multi(bulk_transactional_data.delete_event_ids)
          |> EventsContext.update_all_event_links_multi(bulk_transactional_data.delete_event_ids)
          |> EventsContext.run_multi_transaction()

        case transaction_result do
          {:ok, results} ->
            Enum.each(results, fn {key, {_count, items}} ->
              if String.contains?(to_string(key), "upsert_notifications") do
                SystemNotificationContext.bulk_update_system_notifications(items)
              end
            end)

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_batch_transaction_for_crud/1 failed with reason: #{
                inspect(reason, pretty: true)
              }"
            )

            raise "execute_batch_transaction_for_crud/1 failed"
        end

      false ->
        transaction_result =
          EventsContext.insert_all_event_details_multi(
            multi,
            bulk_transactional_data.event_details
          )
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
          |> EventsContext.update_all_events_multi(bulk_transactional_data.delete_event_ids)
          |> EventsContext.update_all_event_links_multi(bulk_transactional_data.delete_event_ids)
          |> EventsContext.run_multi_transaction()

        case transaction_result do
          {:ok, %{insert_notifications: {_count_created, created_notifications}}} ->
            SystemNotificationContext.bulk_insert_system_notifications(created_notifications)

          {:ok, results} ->
            Enum.each(results, fn {key, {_count, items}} ->
              if String.contains?(to_string(key), "upsert_notifications") do
                SystemNotificationContext.bulk_update_system_notifications(items)
              end

              if key == :insert_notifications do
                SystemNotificationContext.bulk_insert_system_notifications(items)
              end
            end)

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_batch_transaction_for_crud/1 failed with reason: #{
                inspect(reason, pretty: true)
              }"
            )

            raise "execute_batch_transaction_for_crud/1 failed"
        end
    end
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
              v2
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

    messages
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
               core_id: event["id"],
               occurred_at: event["_timestamp"]
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
               core_id: event["id"],
               occurred_at: event["_timestamp"]
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
               core_id: event["id"],
               occurred_at: event["_timestamp"]
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
