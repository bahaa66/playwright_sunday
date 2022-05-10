defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.System.SystemNotificationContext
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi
  alias CogyntWorkstationIngest.Elasticsearch.EventDocumentBuilder

  @defaults %{
    crud_action: nil,
    event_document: nil,
    notification_priority: 3
  }

  @doc """
  process_event/1 for a CRUD: delete event. We just store the core_id of the event that needs
  to be deleted as part of the payload. When it hits the transactional step of the pipeline the
  event and its associated data will be removed otherwise it will build an event map that
  will be processed in bulk in the transactional step of the pipeline
  """
  def process_event(
        %{event: event, event_definition_hash_id: event_definition_hash_id, core_id: core_id} =
          data
      ) do
    action = Map.get(event, Config.crud_key(), nil)
    now = DateTime.truncate(DateTime.utc_now(), :second)

    occurred_at =
      case event[Config.timestamp_key()] do
        nil ->
          nil

        date_string ->
          {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

          dt_struct
          |> DateTime.truncate(:second)
      end

    cond do
      action == Config.crud_delete_value() ->
        Map.put(data, :crud_action, action)
        |> Map.put(:delete_core_id, core_id)
        |> Map.put(:pipeline_state, :process_event)

      action == Config.crud_update_value() ->
        Map.put(data, :pg_event, %{
          core_id: core_id,
          occurred_at: occurred_at,
          risk_score: format_risk_score(event[Config.confidence_key()]),
          event_details: format_lexicon_data(event),
          event_definition_hash_id: event_definition_hash_id,
          created_at: now,
          updated_at: now
        })
        |> Map.put(:crud_action, action)
        |> Map.put(:pipeline_state, :process_event)

      true ->
        Map.put(data, :pg_event, %{
          core_id: core_id,
          occurred_at: occurred_at,
          risk_score: format_risk_score(event[Config.confidence_key()]),
          event_details: format_lexicon_data(event),
          event_definition_hash_id: event_definition_hash_id,
          created_at: now,
          updated_at: now
        })
        |> Map.put(:crud_action, action)
        |> Map.put(:pipeline_state, :process_event)
    end
  end

  @doc """
  process_event_history/1 ....
  """
  def process_event_history(
        %{event: event, event_definition_hash_id: event_definition_hash_id, core_id: core_id} =
          data
      ) do
    action = Map.get(event, Config.crud_key(), nil)

    if is_nil(action) do
    else
      occurred_at =
        case event[Config.timestamp_key()] do
          nil ->
            nil

          date_string ->
            {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

            dt_struct
            |> DateTime.truncate(:second)
        end

      published_at =
        case event[Config.published_at_key()] do
          nil ->
            nil

          date_string ->
            {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

            dt_struct
            |> DateTime.truncate(:second)
        end

      Map.put(data, :pg_event_history, %{
        id: Ecto.UUID.generate(),
        core_id: core_id,
        occurred_at: occurred_at,
        version: event[Config.version_key()],
        crud: action,
        risk_score: format_risk_score(event[Config.confidence_key()]),
        event_details: format_lexicon_data(event),
        event_definition_hash_id: event_definition_hash_id,
        published_at: published_at
      })
    end
  end

  @doc """
  process_elasticsearch_documents/1 will build the Event Elasticsearch document that Workstation
  uses to fetch its search facets and do a lot of quick keyword searches against.
  """
  def process_elasticsearch_documents(%{delete_core_id: _, crud_action: _} = data) do
    data
    |> Map.put(:pipeline_state, :process_elasticsearch_documents)
  end

  def process_elasticsearch_documents(
        %{
          pg_event: pg_event,
          core_id: core_id,
          event_definition: event_definition,
          event_definition_hash_id: event_definition_hash_id,
          event_type: event_type,
          crud_action: action
        } = data
      ) do
    cond do
      action == Config.crud_delete_value() ->
        data

      true ->
        event_definition_details = event_definition.event_definition_details

        # Iterate over each event key value pair and build the pg and elastic search event
        # details.
        elasticsearch_event_details =
          Enum.reduce(pg_event.event_details, [], fn
            {"COG_" <> _, _value}, acc ->
              acc

            {key, value}, acc ->
              # Search the event definition details and use the path to figure out the field value.
              Enum.find_value(event_definition_details, fn
                %{path: path, field_name: field_name, field_type: field_type} ->
                  # Split the path on the delimiter which currently is hard coded to |
                  case String.split(path, "|") do
                    # If there is only one element in the list then we don't need to dig into the object
                    # any further and we return the value.
                    [first] when first == key ->
                      value

                    [first | remaining_path] when first == key ->
                      # If the path is has a length is greater than 1 then whe use it to get the value.
                      Enum.reduce(remaining_path, value, fn
                        p, a when is_map(a) ->
                          Map.get(a, p)

                        _, _ ->
                          nil
                      end)
                      |> case do
                        nil ->
                          # add topoc to log
                          CogyntLogger.warn(
                            "#{__MODULE__}",
                            "Could not find value at given Path: #{inspect(path)}"
                          )

                          false

                        value ->
                          value
                      end

                    _ ->
                      nil
                  end
                  # Convert the value if needed
                  |> case do
                    nil -> false
                    value when is_binary(value) -> {value, field_name, field_type, path}
                    value -> {Jason.encode!(value), field_name, field_type, path}
                  end
              end)
              |> case do
                # If it has a field type then it has a corresponding event definition detail that gives
                # us the the field_type so we save an event_detail and a elastic document
                {field_value, field_name, field_type, path} ->
                  acc ++
                    [
                      %{
                        field_name: field_name,
                        field_type: field_type,
                        field_value: field_value,
                        path: path
                      }
                    ]

                nil ->
                  acc
              end
          end)

        # Build elasticsearch documents
        elasticsearch_event_doc =
          case EventDocumentBuilder.build_document(%{
                 id: core_id,
                 title: event_definition.title,
                 event_definition_id: event_definition.event_definition_id,
                 event_definition_hash_id: event_definition_hash_id,
                 event_details: elasticsearch_event_details,
                 core_event_id: core_id,
                 event_type: event_type,
                 occurred_at: pg_event.occurred_at,
                 risk_score: pg_event.risk_score
               }) do
            {:ok, event_doc} ->
              event_doc

            _ ->
              @defaults.event_document
          end

        Map.put(data, :event_doc, elasticsearch_event_doc)
        |> Map.put(:pipeline_state, :process_elasticsearch_documents)
    end
  end

  @doc """
  process_notifications/1 will build notifications objects for each valid notification_setting
  at the time. With CRUD: update messages we need to take into consideration the possibilty
  of a notification that was previously created against a valid notification setting, no longer
  being valid. So we must also fetch all invalid_notification_settings at the time and store the
  core_ids for the associated notifications to be removed in the transactional step of the pipeline.
  """
  def process_notifications(%{delete_core_id: _, crud_action: _} = data) do
    data
    |> Map.put(:pipeline_state, :process_notifications)
  end

  def process_notifications(
        %{
          pg_event: pg_event,
          core_id: core_id,
          event_definition: event_definition,
          crud_action: action
        } = data
      ) do
    cond do
      action == Config.crud_delete_value() ->
        data

      true ->
        pg_notifications =
          NotificationsContext.fetch_valid_notification_settings(
            %{
              event_definition_hash_id: event_definition.id,
              active: true
            },
            pg_event.risk_score,
            event_definition
          )
          |> Enum.reduce([], fn ns, acc ->
            now = DateTime.truncate(DateTime.utc_now(), :second)

            acc ++
              [
                %{
                  archived_at: nil,
                  priority: @defaults.notification_priority,
                  assigned_to: ns.assigned_to,
                  dismissed_at: nil,
                  core_id: core_id,
                  tag_id: ns.tag_id,
                  notification_setting_id: ns.id,
                  created_at: now,
                  updated_at: now
                }
              ]
          end)

        invalid_notification_settings =
          NotificationsContext.fetch_invalid_notification_settings(
            %{
              event_definition_hash_id: event_definition.id,
              active: true
            },
            pg_event.risk_score,
            event_definition
          )

        pg_notifications_delete =
          if Enum.empty?(invalid_notification_settings) do
            []
          else
            NotificationsContext.query_notifications(%{
              filter: %{
                notification_setting_ids:
                  Enum.map(invalid_notification_settings, fn invalid_ns -> invalid_ns.id end),
                core_id: core_id
              }
            })
            |> Enum.map(fn notifications -> notifications.core_id end)
          end

        Map.put(data, :pg_notifications, pg_notifications)
        |> Map.put(:pg_notifications_delete, pg_notifications_delete)
        |> Map.put(:pipeline_state, :process_notifications)
    end
  end

  @doc """
  Takes all the messages that have gone through the processing steps in the pipeline up to the batch limit
  configured. Will execute one multi transaction to delete and upsert all objects
  """
  def execute_batch_transaction(messages, event_type, pg_event_history \\ []) do
    # build transactional data
    default_map = %{
      pg_event: [],
      pg_notifications: [],
      event_doc: [],
      pg_event_links: [],
      delete_core_id: [],
      pg_notifications_delete: [],
      pg_event_links_delete: []
    }

    bulk_transactional_data =
      Enum.reduce(messages, default_map, fn data, acc ->
        data =
          Map.drop(data, [
            :crud_action,
            :event,
            :event_definition,
            :event_definition_hash_id,
            :retry_count,
            :pipeline_state,
            :pg_event_history
          ])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_doc ->
              if v2 == @defaults.event_document do
                v1
              else
                v1 ++ [v2]
              end

            :pg_event ->
              v1 ++ [v2]

            :pg_notifications ->
              v1 ++ List.flatten(v2)

            :pg_event_links ->
              v1 ++ List.flatten(v2)

            :delete_core_id ->
              v1 ++ [v2]

            :pg_notifications_delete ->
              v1 ++ List.flatten(v2)

            :pg_event_links_delete ->
              v1 ++ List.flatten(v2)

            _ ->
              v2
          end
        end)
      end)
      |> Map.put(:pg_event_history, pg_event_history)

    # Build a Multi transaction to insert all the pg records
    transaction_result =
      Multi.new()
      |> Ecto.Multi.run(:bulk_upsert_event_documents, fn _repo, _ ->
        bulk_upsert_event_documents(bulk_transactional_data)
      end)
      |> NotificationsContext.delete_all_notifications_multi(
        Enum.uniq(
          bulk_transactional_data.delete_core_id ++
            bulk_transactional_data.pg_notifications_delete
        )
      )
      |> EventsContext.delete_all_event_links_multi(
        Enum.uniq(
          bulk_transactional_data.delete_core_id ++ bulk_transactional_data.pg_event_links_delete
        )
      )
      |> EventsContext.delete_all_events_multi(bulk_transactional_data.delete_core_id)
      |> EventsContext.upsert_all_events_multi(bulk_transactional_data.pg_event,
        on_conflict: {:replace_all_except, [:core_id, :created_at]},
        conflict_target: [:core_id]
      )
      |> NotificationsContext.upsert_all_notifications_multi(
        bulk_transactional_data.pg_notifications,
        returning: [
          :core_id,
          :tag_id,
          :id,
          :notification_setting_id,
          :created_at,
          :updated_at,
          :assigned_to
        ],
        on_conflict: {:replace_all_except, [:id, :created_at, :core_id]},
        conflict_target: [:core_id, :notification_setting_id]
      )
      |> EventsContext.upsert_all_event_links_multi(bulk_transactional_data.pg_event_links)
      |> EventsContext.upsert_all_event_history_multi(bulk_transactional_data.pg_event_history,
        on_conflict: {:replace_all_except, [:id, :core_id, :version, :crud]},
        conflict_target: [:core_id, :version, :crud]
      )
      |> Ecto.Multi.run(:bulk_delete_event_documents, fn _repo, _ ->
        bulk_delete_event_documents(bulk_transactional_data)
      end)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok, %{upsert_notifications: {_count_created, upserted_notifications}}} ->
        EventsContext.vacuum_events_table()
        EventsContext.vacuum_event_history_table()
        EventsContext.vacuum_event_links_table()

        Redis.publish_async(
          "events_changed_listener",
          %{
            event_type: event_type,
            deleted: bulk_transactional_data.delete_core_id,
            upserted: bulk_transactional_data.pg_event
          }
        )

        SystemNotificationContext.bulk_insert_system_notifications(upserted_notifications)

      {:ok, _} ->
        EventsContext.vacuum_events_table()
        EventsContext.vacuum_event_history_table()
        EventsContext.vacuum_event_links_table()

        Redis.publish_async(
          "events_changed_listener",
          %{
            event_type: event_type,
            deleted: bulk_transactional_data.delete_core_id,
            upserted: bulk_transactional_data.pg_event
          }
        )

        nil

      {:error, :bulk_upsert_event_documents, :failed, _} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: bulk_upsert_event_documents failed"
        )

        raise "execute_transaction/1 failed"

      {:error, :bulk_delete_event_documents, :failed, _} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: bulk_delete_event_documents failed"
        )

        rollback_event_index_data(bulk_transactional_data)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        rollback_event_index_data(bulk_transactional_data)

        raise "execute_transaction/1 failed"
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp bulk_delete_event_documents(bulk_transactional_data) do
    if !Enum.empty?(bulk_transactional_data.delete_core_id) do
      case ElasticApi.bulk_delete(
             Config.event_index_alias(),
             bulk_transactional_data.delete_core_id
           ) do
        {:ok, _} ->
          {:ok, :success}

        _ ->
          {:error, :failed}
      end
    else
      {:ok, :success}
    end
  end

  defp bulk_upsert_event_documents(bulk_transactional_data) do
    if !Enum.empty?(bulk_transactional_data.event_doc) do
      case ElasticApi.bulk_upsert_document(
             Config.event_index_alias(),
             bulk_transactional_data.event_doc
           ) do
        {:ok, _} ->
          {:ok, :success}

        _ ->
          {:error, :failed}
      end
    else
      {:ok, :success}
    end
  end

  defp rollback_event_index_data(bulk_transactional_data) do
    event_doc_ids =
      bulk_transactional_data.event_doc
      |> Enum.map(fn event_doc -> event_doc.id end)

    ElasticApi.bulk_delete(
      Config.event_index_alias(),
      event_doc_ids
    )
  end

  defp format_lexicon_data(event) do
    case Map.get(event, Config.matches_key()) do
      nil ->
        event

      lexicon_val ->
        try do
          Map.put(
            event,
            Config.matches_key(),
            List.flatten(lexicon_val) |> Enum.filter(&is_binary(&1))
          )
        rescue
          _ ->
            CogyntLogger.error("#{__MODULE__}", "Lexicon value incorrect format #{lexicon_val}")
            Map.delete(event, Config.matches_key())
        end
    end
  end

  defp format_risk_score(nil), do: nil

  defp format_risk_score(risk_score) when is_float(risk_score),
    do: trunc(Float.round(risk_score * 100))

  defp format_risk_score(risk_score) when is_integer(risk_score), do: risk_score * 100

  defp format_risk_score(risk_score) do
    if risk_score != 0 do
      case Float.parse(risk_score) do
        :error ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to parse _confidence as a float. Defaulting to 0"
          )

          0

        {score, _extra} ->
          trunc(Float.round(score * 100))
      end
    else
      risk_score
    end
  end
end
