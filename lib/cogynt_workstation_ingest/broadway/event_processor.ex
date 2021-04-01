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
        data = Map.put(data, :pipeline_state, :process_event)
        Map.put(message, :data, data)
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
        data = Map.put(data, :pipeline_state, :process_event)
        Map.put(message, :data, data)
    end
  end

  @doc """
  Takes the field_name and field_value fields from the event and creates a list of event_detail
  maps. Also creates a list of elasticsearch docs. Returns an updated data map with
  the :event_details, :risk_history_doc and :event_docs values.
  """
  def process_event_details_and_elasticsearch_docs(
        %Message{data: %{event_id: nil} = data} = message
      ) do
    data = Map.put(data, :pipeline_state, :process_event_details_and_elasticsearch_docs)
    Map.put(message, :data, data)
  end

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

            # acc_pg_event_details =
            #   acc_pg_event_details ++
            #     [
            #       %{
            #         event_id: event_id,
            #         field_name: field_name,
            #         field_type: field_type,
            #         field_value: field_value
            #       }
            #     ]

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
  def process_notifications(%Message{data: %{event_id: nil} = data} = message) do
    data = Map.put(data, :pipeline_state, :process_notifications)
    Map.put(message, :data, data)
  end

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
    IO.puts("HERE NO CRUD")
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

    IO.inspect(length(notifications), label: "COUNT OF NOTIFICATIONS CREATED NO CRUD")

    # TODO: create system notifications in bulk step
    SystemNotificationContext.bulk_insert_system_notifications(notifications)

    data = Map.put(data, :pipeline_state, :process_notifications)
    Map.put(message, :data, data)
  end

  def process_notifications(
        %Message{
          data:
            %{
              event: event,
              event_definition: event_definition,
              event_id: event_id,
              crud_action: crud_action,
              deleted_event_ids: deleted_event_ids
            } = data
        } = message
      ) do
    IO.puts("HERE")
    IO.inspect(deleted_event_ids, label: "DELETED_EVENT_IDS")

    case Enum.empty?(deleted_event_ids) do
      true ->
        data = Map.put(data, :pipeline_state, :process_notifications)
        Map.put(message, :data, data)

      false ->
        # start = Time.utc_now()
        risk_score = Map.get(event, @risk_score, 0)

        # 1) Fetch all valid_notification_settings and build a list of notifications to
        # create for each
        new_notifications =
          NotificationsContext.fetch_valid_notification_settings(
            %{
              event_definition_id: event_definition.id,
              deleted_at: nil,
              active: true
            },
            risk_score,
            event_definition
          )
          |> Enum.reduce([], fn valid_ns, acc ->
            # Set fields for Stream_input
            deleted_at =
              if crud_action == @delete do
                DateTime.truncate(DateTime.utc_now(), :second)
              else
                nil
              end

            now = DateTime.truncate(DateTime.utc_now(), :second)

            acc ++
              [
                %{
                  id: Ecto.UUID.generate(),
                  title: valid_ns.title,
                  # description: nil,
                  user_id: valid_ns.user_id,
                  archived_at: nil,
                  priority: nil,
                  assigned_to: valid_ns.assigned_to,
                  dismissed_at: nil,
                  deleted_at: deleted_at,
                  core_id: event["id"],
                  event_id: event_id,
                  tag_id: valid_ns.tag_id,
                  notification_setting_id: valid_ns.id,
                  created_at: now,
                  updated_at: now
                }
              ]
          end)

        # 2) Update all notifications that match invalid_notification_settings to be deleted
        invalid_notification_setting_ids =
          NotificationsContext.fetch_invalid_notification_settings(
            %{
              event_definition_id: event_definition.id,
              deleted_at: nil,
              active: true
            },
            risk_score,
            event_definition
          )
          |> Enum.map(fn ns -> ns.id end)

        if !Enum.empty?(invalid_notification_setting_ids) do
          NotificationsContext.update_notifcations(
            %{
              filter: %{
                notification_setting_ids: invalid_notification_setting_ids,
                deleted_at: nil,
                core_id: event["id"]
              }
            },
            set: [deleted_at: DateTime.truncate(DateTime.utc_now(), :second)]
          )
        end

        # 3) Insert notifications for valid_notification_settings
        {_count, created_notifications} =
          NotificationsContext.insert_all_notifications(new_notifications,
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
            ],
            on_conflict:
              {:replace,
               [
                 :title,
                 # :description,
                 :user_id,
                 :assigned_to,
                 :deleted_at,
                 :event_id,
                 :notification_setting_id,
                 :tag_id,
                 :core_id,
                 :updated_at
               ]},
            conflict_target:
              {:unsafe_fragment, "(core_id, notification_setting_id) WHERE core_id IS NOT NULL"}
          )

        IO.inspect(length(created_notifications), label: "COUNT OF NOTIFICATIONS CREATED")

        if !Enum.empty?(created_notifications) do
          # TODO: create system notifications in bulk step
          SystemNotificationContext.bulk_insert_system_notifications(created_notifications)
        end

        # finish = Time.utc_now()
        # diff = Time.diff(finish, start, :millisecond)
        # IO.puts("DURATION OF NEW NOTIFICATION LOGIC: #{diff}, PID: #{inspect(self())}")

        data = Map.put(data, :pipeline_state, :process_notifications)
        Map.put(message, :data, data)
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

    # Elasticsearch Transactional Upserts
    bulk_upsert_event_documents_with_transaction(bulk_transactional_data)
    bulk_upsert_risk_history_with_transaction(bulk_transactional_data)

    case EventsContext.insert_all_event_details_with_copy(bulk_transactional_data.event_details) do
      {:ok, _} ->
        nil

      _ ->
        rollback_all_elastic_index_data(bulk_transactional_data)
        raise "execute_batch_transaction_for_crud/1 failed"
    end

    # EventDetails Transactional Inserts
    # try do
    #   EventsContext.insert_all_event_details(bulk_transactional_data.event_details)
    # rescue
    #   _ ->
    #     rollback_all_elastic_index_data(bulk_transactional_data)
    #     raise "execute_batch_transaction_for_crud/1 failed"
    # end
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

    # Elasticsearch Transactional Upserts
    bulk_upsert_event_documents_with_transaction(bulk_transactional_data)
    bulk_upsert_risk_history_with_transaction(bulk_transactional_data)

    case EventsContext.insert_all_event_details_with_copy(bulk_transactional_data.event_details) do
      {:ok, _} ->
        nil

      _ ->
        rollback_all_elastic_index_data(bulk_transactional_data)
        raise "execute_batch_transaction/1 failed"
    end

    # EventDetails Transactional Inserts
    # try do
    #   EventsContext.insert_all_event_details(bulk_transactional_data.event_details)
    # rescue
    #   _ ->
    #     rollback_all_elastic_index_data(bulk_transactional_data)
    #     raise "execute_batch_transaction/1 failed"
    # end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp bulk_upsert_event_documents_with_transaction(bulk_transactional_data) do
    if !Enum.empty?(Map.get(bulk_transactional_data, :deleted_event_ids, [])) do
      if !Enum.empty?(bulk_transactional_data.event_doc) do
        # Filter out deleted_event_ids from the event_doc before insert
        event_docs =
          bulk_transactional_data.event_doc
          |> Enum.filter(fn event_doc ->
            !Enum.member?(bulk_transactional_data.deleted_event_ids, event_doc.id)
          end)

        case Elasticsearch.bulk_upsert_document(Config.event_index_alias(), event_docs) do
          {:ok, _} ->
            :ok

          _ ->
            rollback_event_index_data(bulk_transactional_data)

            raise "bulk_upsert_event_documents_with_transaction/1 failed"
        end
      end
    else
      if !Enum.empty?(bulk_transactional_data.event_doc) do
        case Elasticsearch.bulk_upsert_document(
               Config.event_index_alias(),
               bulk_transactional_data.event_doc
             ) do
          {:ok, _} ->
            :ok

          _ ->
            rollback_event_index_data(bulk_transactional_data)

            raise "bulk_upsert_event_documents_with_transaction/1 failed"
        end
      end
    end
  end

  defp bulk_upsert_risk_history_with_transaction(bulk_transactional_data) do
    if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
      case Elasticsearch.bulk_upsert_document(
             Config.risk_history_index_alias(),
             bulk_transactional_data.risk_history_doc
           ) do
        {:ok, _} ->
          :ok

        _ ->
          rollback_risk_history_index_data(bulk_transactional_data)

          raise "bulk_upsert_risk_history_with_transaction/1 failed"
      end
    end
  end

  defp rollback_event_index_data(bulk_transactional_data) do
    event_doc_ids =
      bulk_transactional_data.event_doc
      |> Enum.map(fn event_doc -> event_doc.id end)

    Elasticsearch.bulk_delete_document(
      Config.event_index_alias(),
      event_doc_ids
    )
  end

  defp rollback_risk_history_index_data(bulk_transactional_data) do
    risk_history_doc_ids =
      bulk_transactional_data.risk_history_doc
      |> Enum.map(fn risk_history_doc -> risk_history_doc.id end)

    Elasticsearch.bulk_delete_document(
      Config.risk_history_index_alias(),
      risk_history_doc_ids
    )
  end

  defp rollback_all_elastic_index_data(bulk_transactional_data) do
    rollback_event_index_data(bulk_transactional_data)
    rollback_risk_history_index_data(bulk_transactional_data)
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
