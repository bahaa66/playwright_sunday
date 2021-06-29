defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Elasticsearch.DocumentBuilders.{EventDocumentBuilder, RiskHistoryDocumentBuilder}
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.System.SystemNotificationContext

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @lexicons Application.get_env(:cogynt_workstation_ingest, :core_keys)[:lexicons]
  @defaults %{
    deleted_event_ids: [],
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil,
    event_id: nil,
    notification_priority: 3
  }

  # TODO: update events table schema
  @doc """
  Creates the map from the ingested Kafka metadata that will be inserted into the Events table
  """
  def process_event(%{event: event, event_definition_id: event_definition_id} = data) do
    action = event[@crud]

    pg_event =
      case action do
        "update" ->
          %{
            core_id: event["id"],
            occurred_at: event["_timestamp"],
            risk_score: format_risk_score(event["_confidence"]),
            event_details: Jason.encode!(event),
            event_definition_id: event_definition_id,
            updated_at: DateTime.truncate(DateTime.utc_now(), :second)
          }

        "delete" ->
          %{
            core_id: event["id"],
            risk_score: format_risk_score(event["_confidence"]),
            occurred_at: event["_timestamp"],
            event_details: Jason.encode!(event),
            event_definition_id: event_definition_id
          }

        _ ->
          now = DateTime.truncate(DateTime.utc_now(), :second)

          %{
            core_id: event["id"],
            occurred_at: event["_timestamp"],
            risk_score: format_risk_score(event["_confidence"]),
            event_details: Jason.encode!(event),
            event_definition_id: event_definition_id,
            created_at: now,
            updated_at: now
          }
      end

    Map.put(data, :pg_event, pg_event)
    |> Map.put(:crud_action, action)
    |> Map.put(:pipeline_state, :process_event)
  end

  @doc """
  """
  def process_elasticsearch_documents(
        %{
          event: event,
          event_definition: event_definition,
          event_definition_id: event_definition_id,
          crud_action: action
        } = data
      ) do
    event = format_lexicon_data(event)
    core_id = event["id"]
    published_at = event["published_at"]
    event_definition_details = event_definition.event_definition_details

    # Iterate over each event key value pair and build the pg and elastic search event
    # details.
    elasticsearch_event_details =
      Enum.reduce(event, [], fn
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
                value when is_binary(value) -> {value, field_name, field_type}
                value -> {Jason.encode!(value), field_name, field_type}
              end
          end)
          |> case do
            # If it has a field type then it has a corresponding event definition detail that gives
            # us the the field_type so we save an event_detail and a elastic document
            {field_value, field_name, field_type} ->
              acc ++
                [
                  %{
                    field_name: field_name,
                    field_type: field_type,
                    field_value: field_value
                  }
                ]

            nil ->
              acc
          end
      end)

    # Build elasticsearch documents
    elasticsearch_event_doc =
      if action != @delete do
        case EventDocumentBuilder.build_document(
               core_id,
               event_definition.title,
               event_definition_id,
               elasticsearch_event_details,
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

    Map.put(data, :event_doc, elasticsearch_event_doc)
    |> Map.put(:pipeline_state, :process_elasticsearch_documents)
  end

  @doc """
  """
  def process_notifications(
        %{
          event: event,
          event_definition: event_definition
        } = data
      ) do
    risk_score = Map.get(event, @risk_score, 0)

    pg_notifications =
      NotificationsContext.fetch_valid_notification_settings(
        %{
          event_definition_id: event_definition.id,
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
              archived_at: nil,
              priority: @defaults.notification_priority,
              assigned_to: ns.assigned_to,
              dismissed_at: nil,
              core_id: event["id"],
              tag_id: ns.tag_id,
              notification_setting_id: ns.id,
              created_at: now,
              updated_at: now
            }
          ]
      end)

    Map.put(data, :pg_notifications, pg_notifications)
    |> Map.put(:pipeline_state, :process_notifications)
  end

  @doc """
  For datasets that can be processed in bulk this will aggregate all of the
  processed data and insert it into postgres in bulk
  """
  def execute_batch_transaction(messages) do
    # build transactional data
    default_map = %{
      pg_event: [],
      notifications: [],
      event_doc: [],
      risk_history_doc: []
    }

    bulk_transactional_data =
      Enum.reduce(messages, default_map, fn data, acc ->
        data =
          Map.drop(data, [
            :crud_action,
            :event,
            :event_definition,
            :event_definition_id,
            :retry_count
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
              v1 ++ [v2]

            _ ->
              v2
          end
        end)
      end)

    # Elasticsearch Transactional Upserts
    bulk_upsert_event_documents_with_transaction(bulk_transactional_data)
    bulk_upsert_risk_history_with_transaction(bulk_transactional_data)

    # Build a Multi transaction to insert all the pg records
    transaction_result =
      EventsContext.upsert_all_events_multi(bulk_transactional_data.pg_event,
        on_conflict:
          {:replace,
           [:occurred_at, :risk_score, :event_details, :event_definition_id, :updated_at]},
        conflict_target: [:core_id]
      )
      |> NotificationsContext.upsert_all_notifications_multi(
        bulk_transactional_data.pg_notifications,
        returning: [
          :core_id,
          :tag_id,
          :id,
          :title,
          :notification_setting_id,
          :created_at,
          :updated_at,
          :assigned_to
        ],
        on_conflict: {:replace, [:tag_id, :title, :updated_at, :assigned_to, :updated_at]},
        conflict_target: [:core_id, :notification_setting_id]
      )
      # TODO: add on_conflict
      |> EventsContext.upsert_all_event_links_multi(bulk_transactional_data.pg_event_links)
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

        rollback_all_elastic_index_data(bulk_transactional_data)

        raise "execute_transaction/1 failed"
    end
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

  defp format_risk_score(nil), do: nil

  defp format_risk_score(confidence) do
    if confidence != 0 do
      case Float.parse(confidence) do
        :error ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Failed to parse _confidence as a float. Defaulting to 0"
          )

          0

        {score, _extra} ->
          score
      end
    else
      confidence
    end
  end
end
