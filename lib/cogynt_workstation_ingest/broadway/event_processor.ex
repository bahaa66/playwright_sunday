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
    deleted_event_ids: nil,
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil,
    event_id: nil
  }

  @doc """
  Will create the event record for the Broadway Message. If Crud action key exists
  then process_event() will call a Psql Function to delete events linked to the core_id.
  Otherwise it just inserts the event.
  """
  def process_event(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "process_event/1 failed. No message data")
    message
  end

  def process_event(
        %Message{
          data:
            %{
              event_id: event_id,
              event: %{@crud => action} = event,
              event_definition_id: event_definition_id
            } = data
        } = message
      ) do
    case is_nil(event_id) do
      true ->
        core_id = event["id"]
        occurred_at = event["_timestamp"]
        event_id = Ecto.UUID.generate()

        # If Crud action is Delete, then we need to set the deleted_at column
        # of the event we are creating and add the event_id into the list
        # of deleted_event_ids
        deleted_at =
          if action == @delete do
            DateTime.truncate(DateTime.utc_now(), :second)
          else
            nil
          end

        case EventsContext.call_insert_crud_event_function(
               event_id,
               event_definition_id,
               core_id,
               occurred_at,
               deleted_at
             ) do
          {:ok, %Postgrex.Result{rows: rows}} ->
            deleted_event_ids =
              List.flatten(rows)
              |> Enum.reduce([], fn binary_id, acc ->
                case Ecto.UUID.cast(binary_id) do
                  {:ok, uuid} ->
                    acc ++ [uuid]

                  _ ->
                    acc
                end
              end)

            data =
              Map.put(data, :event_id, event_id)
              |> Map.put(:deleted_event_ids, deleted_event_ids)
              |> Map.put(:crud_action, action)
              |> Map.put(:pipeline_state, :process_event)

            Map.put(message, :data, data)

          {:error, %Postgrex.Error{postgres: %{message: error}}} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "insert_event failed with Error: #{inspect(error)}"
            )

            raise "process_event/1 failed"

          _ ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "insert_event failed"
            )

            raise "process_event/1 failed"
        end

      false ->
        message
    end
  end

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
              |> Map.put(:deleted_event_ids, @defaults.deleted_event_ids)
              |> Map.put(:crud_action, @defaults.crud_action)
              |> Map.put(:pipeline_state, :process_event)

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
    {pg_event_details, elastic_event_details} =
      Enum.reduce(event, {[], []}, fn {field_name, field_value},
                                      {acc_pg_event_details, acc_elastic_event_document} ->
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

            acc_pg_event_details =
              acc_pg_event_details ++
                [
                  "\"#{field_name}\";\"#{field_value}\";\"#{field_type}\";#{event_id}\n"
                ]

            acc_elastic_event_document =
              if not is_nil(field_type) do
                acc_elastic_event_document ++
                  [
                    %{
                      event_id: event_id,
                      field_name: field_name,
                      field_type: field_type,
                      field_value: field_value
                    }
                  ]
              else
                acc_elastic_event_document
              end

            {acc_pg_event_details, acc_elastic_event_document}

          true ->
            {acc_pg_event_details, acc_elastic_event_document}
        end
      end)

    # Build elasticsearch documents
    elasticsearch_event_doc =
      if action != @delete do
        case EventDocumentBuilder.build_document(
               event_id,
               core_id,
               event_definition.title,
               event_definition_id,
               elastic_event_details,
               published_at
             ) do
          {:ok, event_doc} ->
            event_doc

          _ ->
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
      Map.put(data, :event_details, pg_event_details)
      |> Map.put(:event_doc, elasticsearch_event_doc)
      |> Map.put(:risk_history_doc, elasticsearch_risk_history_doc)
      |> Map.put(:pipeline_state, :process_event_details_and_elasticsearch_docs)

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
              deleted_event_ids: nil
            } = data
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

        # Message coming through pipeline can be a failed retry message
        # need to ensure Notifications may not already have been created since
        # they were removed from the transactional step
        if is_nil(
             NotificationsContext.get_notification_by(
               event_id: event_id,
               notification_setting_id: ns.id
             )
           ) do
          acc ++
            [
              %{
                title: ns.title,
                # description: nil,
                user_id: ns.user_id,
                archived_at: nil,
                priority: nil,
                assigned_to: ns.assigned_to,
                dismissed_at: nil,
                deleted_at: nil,
                core_id: event["id"],
                event_id: event_id,
                tag_id: ns.tag_id,
                notification_setting_id: ns.id,
                created_at: now,
                updated_at: now
              }
            ]
        else
          acc
        end
      end)
      |> NotificationsContext.insert_all_notifications(
        returning: [
          :id,
          :title,
          # :description,
          :user_id,
          :archived_at,
          :priority,
          :assigned_to,
          :dismissed_at,
          :deleted_at,
          :core_id,
          :event_id,
          :tag_id,
          :notification_setting_id,
          :created_at,
          :updated_at
        ]
      )

    SystemNotificationContext.bulk_insert_system_notifications(notifications)

    # Update pipeline_state to show latest succesfull step passed
    data = Map.put(data, :pipeline_state, :process_notifications)
    Map.put(message, :data, data)
  end

  # TODO: Do we need to use inser_with_copy functions here ?
  def process_notifications(
        %Message{
          data:
            %{
              event: event,
              event_definition: event_definition,
              event_id: event_id,
              deleted_event_ids: deleted_event_ids
            } = data
        } = message
      ) do
    case Enum.empty?(deleted_event_ids) do
      true ->
        message

      false ->
        start = Time.utc_now()

        risk_score = Map.get(event, @risk_score, 0)
        crud_action = Map.get(event, @crud, @defaults.crud_action)

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

        # Second fetch all the Notifications that were created against the deleted_event_ids
        # and create a new list of notifications to either be updated or deleted based on the
        # list of valid_notification_settings
        notifications =
          NotificationsContext.query_notifications(%{
            filter: %{event_ids: deleted_event_ids},
            select: Notification.__schema__(:fields)
          })
          |> Enum.reduce([], fn notification, acc ->
            ns_matched =
              Enum.find(valid_notification_settings, fn notification_setting ->
                notification.notification_setting_id == notification_setting.id
              end)

            # If the notification's notification_setting_id does not match any of the ids
            # from any of the current valid_notification_settings then we must mark the notification
            # as deleted. Then create new notifications for all valid notification settings
            # If there is a match we just update the notification to the new event_id
            if is_nil(ns_matched) do
              # Set fields for Stream_input
              title = notification.title || 'null'
              description = notification.description || 'null'
              user_id = notification.user_id || 'null'
              archived_at = notification.archived_at || 'null'
              priority = notification.priority || 'null'
              assigned_to = notification.assigned_to || 'null'
              dismissed_at = notification.dismissed_at || 'null'
              now = DateTime.truncate(DateTime.utc_now(), :second)
              core_id = event["id"] || 'null'

              deleted_notification = [
                "#{notification.id};#{title};#{description};#{user_id};#{archived_at};#{priority};#{
                  assigned_to
                };#{dismissed_at};#{now};#{notification.event_id};#{
                  notification.notification_setting_id
                };#{notification.tag_id};#{core_id};#{notification.created_at};#{now}\n"
              ]

              new_notifications =
                Enum.reduce(valid_notification_settings, [], fn ns, acc_0 ->
                  # Message coming through pipeline can be a failed retry message
                  # need to ensure Notifications may not already have been created since
                  # they were removed from the transactional step
                  if is_nil(
                       NotificationsContext.get_notification_by(
                         event_id: event_id,
                         notification_setting_id: ns.id
                       )
                     ) do
                    # Set fields for Stream_input
                    id = Ecto.UUID.generate()
                    title = ns.title || 'null'
                    description = 'null'
                    user_id = ns.user_id || 'null'
                    archived_at = 'null'
                    priority = 'null'
                    assigned_to = ns.assigned_to || 'null'
                    dismissed_at = 'null'
                    deleted_at = 'null'
                    core_id = event["id"] || 'null'
                    now = DateTime.truncate(DateTime.utc_now(), :second)

                    acc_0 ++
                      [
                        "#{id};#{title};#{description};#{user_id};#{archived_at};#{priority};#{
                          assigned_to
                        };#{dismissed_at};#{deleted_at};#{event_id};#{ns.id};#{ns.tag_id};#{
                          core_id
                        }#{now};#{now}\n"
                      ]
                  else
                    acc_0
                  end
                end)

              acc ++ deleted_notification ++ new_notifications
            else
              # Message coming through pipeline can be a failed retry message
              # need to ensure Notifications may not already have been created since
              # they were removed from the transactional step
              if is_nil(
                   NotificationsContext.get_notification_by(
                     event_id: event_id,
                     notification_setting_id: ns_matched.id
                   )
                 ) do
                # Set fields for Stream_input
                deleted_at =
                  if crud_action == @delete do
                    DateTime.truncate(DateTime.utc_now(), :second)
                  else
                    'null'
                  end

                title = ns_matched.title || 'null'
                description = notification.description || 'null'
                user_id = ns_matched.user_id || 'null'
                archived_at = notification.archived_at || 'null'
                priority = notification.priority || 'null'
                assigned_to = ns_matched.assigned_to || 'null'
                dismissed_at = notification.dismissed_at || 'null'
                core_id = event["id"] || 'null'
                now = DateTime.truncate(DateTime.utc_now(), :second)

                acc ++
                  [
                    "#{notification.id};#{title};#{description};#{user_id};#{archived_at};#{
                      priority
                    };#{assigned_to};#{dismissed_at};#{deleted_at};#{event_id};#{ns_matched.id};#{
                      ns_matched.tag_id
                    };#{core_id};#{notification.created_at};#{now}\n"
                  ]
              else
                acc
              end
            end
          end)

        if !Enum.empty?(notifications) do
          case NotificationsContext.insert_all_notifications_with_copy(notifications) do
            {:ok, %Postgrex.Result{rows: []}} ->
              finish = Time.utc_now()
              diff = Time.diff(finish, start, :millisecond)
              IO.puts("DURATION OF NEW NOTIFICATION LOGIC: #{diff}, PID: #{inspect(self())}")

              # Update pipeline_state to show latest succesfull step passed
              data = Map.put(data, :pipeline_state, :process_notifications)
              Map.put(message, :data, data)

            {:ok, %Postgrex.Result{rows: results}} ->
              NotificationsContext.map_postgres_results(results)
              |> SystemNotificationContext.bulk_insert_system_notifications()

              finish = Time.utc_now()
              diff = Time.diff(finish, start, :millisecond)
              IO.puts("DURATION OF NEW NOTIFICATION LOGIC: #{diff}, PID: #{inspect(self())}")

              # Update pipeline_state to show latest succesfull step passed
              data = Map.put(data, :pipeline_state, :process_notifications)
              Map.put(message, :data, data)

            {:error, %Postgrex.Error{postgres: %{message: error}}} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "process_notifications failed with Error: #{inspect(error)}"
              )

              raise "procesprocess_notificationss_event/1 failed"

            _ ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "process_notifications failed"
              )

              raise "process_notifications/1 failed"
          end
        end
    end
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
      deleted_event_ids: [],
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

                :deleted_event_ids ->
                  if v2 == @defaults.deleted_event_ids or Enum.empty?(v2) do
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
      deleted_event_ids: [],
      event_details: []
    }

    bulk_transactional_data =
      Enum.reduce(core_id_data_map, default_map, fn {_key, data}, acc ->
        data = Map.drop(data, [:event_id])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :deleted_event_ids ->
              if v2 == @defaults.deleted_event_ids or Enum.empty?(v2) do
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

    case EventsContext.insert_all_event_details_with_copy(bulk_transactional_data.event_details) do
      {:ok, _} ->
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

        if !Enum.empty?(bulk_transactional_data.deleted_event_ids) do
          {:ok, _result} =
            Elasticsearch.bulk_delete_document(
              Config.event_index_alias(),
              bulk_transactional_data.deleted_event_ids
            )
        end

        if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
          {:ok, _} =
            Elasticsearch.bulk_upsert_document(
              Config.risk_history_index_alias(),
              bulk_transactional_data.risk_history_doc
            )
        end

      {:error, _} ->
        raise "execute_batch_transaction_for_crud/1 failed"
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
            :deleted_event_ids,
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

    case EventsContext.insert_all_event_details_with_copy(bulk_transactional_data.event_details) do
      {:ok, _} ->
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

        messages

      {:error, _} ->
        raise "execute_batch_transaction/1 failed"
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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
