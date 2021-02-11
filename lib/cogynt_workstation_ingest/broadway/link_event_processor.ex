defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.System.SystemNotificationContext
  alias CogyntWorkstationIngest.Config
  alias Models.Notifications.Notification

  alias Broadway.Message
  alias Ecto.Multi

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
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
  For datasets that have $CRUD keys present. This data needs
  more pre processing to be done before it can be processed in
  bulk
  """
  def execute_batch_transaction_for_crud(core_id_data_map) do
    # build transactional data
    default_map = %{
      event_details: [],
      link_events: [],
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
                :retry_count,
                :validated
              ])

            Map.merge(acc_1, data, fn k, v1, v2 ->
              case k do
                :event_details ->
                  v1 ++ v2

                :link_events ->
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
      link_events: [],
      notifications: []
    }

    bulk_transactional_data =
      Enum.reduce(core_id_data_map, default_map, fn {_key, data}, acc ->
        data = Map.drop(data, [:event_id])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :link_events ->
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
                filter: %{event_definition_id: event_definition_id}
              })
              |> Enum.filter(fn notification_setting ->
                NotificationsContext.in_risk_range?(
                  event_risk_score,
                  notification_setting.risk_range
                )
              end)

            # Second fetch all the Notifications that were created against the deleted_event_ids
            # and create a new list of notifications to either be updated or deleted based on the
            # list of valid_notification_settings
            notifications =
              NotificationsContext.query_notifications(%{
                filter: %{event_ids: delete_event_ids},
                select: [Notification.__schema__(:fields)]
              })
              |> Enum.reduce([], fn notification, acc ->
                ns =
                  Enum.find(valid_notification_settings, fn notification_setting ->
                    notification.notification_setting_id == notification_setting.id
                  end)

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
          |> EventsContext.insert_all_event_links_multi(bulk_transactional_data.link_events)
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
