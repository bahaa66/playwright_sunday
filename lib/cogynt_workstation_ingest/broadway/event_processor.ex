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
      case RiskHistoryDocumentBuilder.build_document(
             event_id,
             event_definition_id,
             core_id,
             confidence,
             timestamp
           ) do
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
  process_notifications/1 will create the notifications for each event that comes through the pipeline
  due to the nature of notification and CRUD events, the notifications must be created in the database at
  this stage of the pipeline and not in the batch_execution stage.
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
            } = _data
        } = message
      ) do
    risk_score = Map.get(event, @risk_score, 0)

    {_count, notifications} =
      NotificationsContext.fetch_valid_notification_settings(
        %{
          event_definition_id: event_definition.id,
          deleted_at: nil,
          active: true
        },
        risk_score,
        event_definition
      )
      |> Enum.reduce([], fn ns, acc ->
        now = DateTime.truncate(DateTime.utc_now(), :second)

        if is_nil(
             NotificationsContext.get_notification_by(
               event_id: event_id,
               notification_setting_id: ns.id
             )
           ) do
          acc ++
            [
              %{
                event_id: event_id,
                user_id: ns.user_id,
                assigned_to: ns.assigned_to,
                tag_id: ns.tag_id,
                title: ns.title,
                notification_setting_id: ns.id,
                created_at: now,
                updated_at: now
              }
            ]
        else
          acc
        end
      end)
      |> NotificationsContext.bulk_insert_notifications(
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

    SystemNotificationContext.bulk_insert_system_notifications(notifications)

    message
  end

  def process_notifications(
        %Message{
          data:
            %{
              event: event,
              event_definition: event_definition,
              event_id: event_id,
              delete_event_ids: delete_event_ids
            } = _data
        } = message
      ) do
    case Enum.empty?(delete_event_ids) do
      true ->
        message

      false ->
        # start = Time.utc_now()

        risk_score = Map.get(event, @risk_score, 0)
        crud_action = Map.get(event, @crud, @defaults.crud_action)

        if risk_score > 0 do
          IO.puts("RiskScore: #{risk_score}, PID: #{inspect(self)}")
        end

        # First find fetch all the Notification_settings for the EventDefinitionId and
        # filter out all invalid notification_settings. Only notification_settings that match the current
        # events criteria
        valid_notification_settings =
          NotificationsContext.fetch_valid_notification_settings(
            %{
              event_definition_id: event_definition.id,
              deleted_at: nil,
              active: true
            },
            risk_score,
            event_definition
          )

        if risk_score > 0 do
          IO.puts(
            "ValidSettings: #{Enum.count(valid_notification_settings)}, PID: #{inspect(self)}"
          )
        end

        # Second fetch all the Notifications that were created against the deleted_event_ids
        # and create a new list of notifications to either be updated or deleted based on the
        # list of valid_notification_settings
        old_notifications =
          NotificationsContext.query_notifications(%{
            filter: %{event_ids: delete_event_ids},
            select: Notification.__schema__(:fields)
          })

        if risk_score > 0 do
          IO.puts("Old_Notifications: #{Enum.count(old_notifications)}, PID: #{inspect(self)}")
        end

        notifications =
          if Enum.empty?(old_notifications) do
            # A Valid Notification Setting Exists and this event has old_events, however those events never
            # had notifications generated for them. Generate the notifications for the first time here.
            Enum.reduce(valid_notification_settings, [], fn notification_setting, acc ->
              if is_nil(
                   NotificationsContext.get_notification_by(
                     event_id: event_id,
                     notification_setting_id: notification_setting.id
                   )
                 ) do
                acc ++
                  [
                    %{
                      event_id: event_id,
                      user_id: notification_setting.user_id,
                      assigned_to: notification_setting.assigned_to,
                      tag_id: notification_setting.tag_id,
                      title: notification_setting.title,
                      notification_setting_id: notification_setting.id,
                      created_at: DateTime.truncate(DateTime.utc_now(), :second),
                      updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                    }
                  ]
              else
                acc
              end
            end)
          else
            Enum.reduce(old_notifications, [], fn notification, acc ->
              ns_matched =
                Enum.find(valid_notification_settings, fn notification_setting ->
                  notification.notification_setting_id == notification_setting.id
                end)

              # If the notification's notification_setting_id does not match any of the ids
              # from any of the current valid_notification_settings then we must mark the notification
              # as deleted. Then create new notifications for all valid notification settings
              # If there is a match we just update the notification to the new event_id
              if is_nil(ns_matched) do
                if risk_score > 0 do
                  IO.puts("Valid NS did not match old notification. PID: #{inspect(self)}")
                end

                deleted_notification = %{
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
                }

                new_notifications =
                  Enum.reduce(valid_notification_settings, [], fn notification_setting, acc_0 ->
                    if is_nil(
                         NotificationsContext.get_notification_by(
                           event_id: event_id,
                           notification_setting_id: notification_setting.id
                         )
                       ) do
                      acc_0 ++
                        [
                          %{
                            event_id: event_id,
                            user_id: notification_setting.user_id,
                            assigned_to: notification_setting.assigned_to,
                            tag_id: notification_setting.tag_id,
                            title: notification_setting.title,
                            notification_setting_id: notification_setting.id,
                            created_at: DateTime.truncate(DateTime.utc_now(), :second),
                            updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                          }
                        ]
                    else
                      IO.puts("Notification already exists for event_id/notification_setting_id")

                      acc_0
                    end
                  end)

                if risk_score > 0 do
                  IO.puts(
                    "Deleted Notification #{Enum.count([deleted_notification])}. PID: #{
                      inspect(self)
                    }"
                  )

                  IO.puts(
                    "New Notifications #{Enum.count(new_notifications)}. PID: #{inspect(self)}"
                  )
                end

                acc ++ [deleted_notification] ++ new_notifications
              else
                if is_nil(
                     NotificationsContext.get_notification_by(
                       event_id: event_id,
                       notification_setting_id: ns_matched.id
                     )
                   ) do
                  deleted_at =
                    if crud_action == @delete do
                      DateTime.truncate(DateTime.utc_now(), :second)
                    else
                      nil
                    end

                  if risk_score > 0 do
                    IO.puts("Creating Notification for matched NS. PID: #{inspect(self)}")
                  end

                  acc ++
                    [
                      %{
                        id: notification.id,
                        title: ns_matched.title,
                        # description: notification.description,
                        user_id: ns_matched.user_id,
                        archived_at: notification.archived_at,
                        priority: notification.priority,
                        assigned_to: ns_matched.assigned_to,
                        dismissed_at: notification.dismissed_at,
                        deleted_at: deleted_at,
                        event_id: event_id,
                        notification_setting_id: ns_matched.id,
                        tag_id: ns_matched.tag_id,
                        created_at: notification.created_at,
                        updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                      }
                    ]
                else
                  IO.puts("Notification already exists for event_id/notification_setting_id")
                  acc
                end
              end
            end)
          end

        # finish = Time.utc_now()
        # diff = Time.diff(finish, start, :millisecond)
        # IO.puts("DURATION OF NEW NOTIFICATION LOGIC: #{diff}, PID: #{inspect(self())}")

        if risk_score > 0 do
          IO.puts(
            "Notifications To Be Created 4 CRUD: #{Enum.count(notifications)}, PID: #{
              inspect(self)
            }"
          )
        end

        {_count, created_notifications} =
          NotificationsContext.bulk_insert_notifications(
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
            ],
            on_conflict: :replace_all
          )

        SystemNotificationContext.bulk_insert_system_notifications(created_notifications)
    end

    message
  end

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
      event_id: nil,
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
                :event,
                :event_definition_id,
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
                          event_definition_id: v2.event_definition_id,
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
      event_details: []
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

    # Elasticsearch Updates
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
    transaction_result =
      EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
      |> EventsContext.update_all_events_multi(bulk_transactional_data.delete_event_ids)
      |> EventsContext.update_all_event_links_multi(bulk_transactional_data.delete_event_ids)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_batch_transaction_for_crud/1 failed with reason: #{
            inspect(reason, pretty: true)
          }"
        )

        raise "execute_batch_transaction_for_crud/1 failed"

      _ ->
        nil
    end
  end

  @doc """
  For datasets that can be processed in bulk this will aggregate all of the
  processed data and insert it into postgres in bulk
  """
  def execute_batch_transaction(messages) do
    # build transactional data
    default_map = %{
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

    EventsContext.insert_all_event_details(bulk_transactional_data.event_details)

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
