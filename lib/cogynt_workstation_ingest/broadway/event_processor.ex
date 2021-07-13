defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Elasticsearch.DocumentBuilders.EventDocumentBuilder
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.System.SystemNotificationContext

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
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

  @doc """
  Creates the map from the ingested Kafka metadata that will be inserted into the Events table
  """
  def process_event(%{event: %{@crud => "delete"}} = data) do
    # Delete notifications for core_id
    # Delete Events for core_id
    # Delete Event Links for core_id IF event_type is Linkage
    Map.put(data, :crud_action, "delete")
    |> Map.put(:pipeline_state, :process_event)
  end

  def process_event(
        %{event: event, event_definition_id: event_definition_id, core_id: core_id} = data
      ) do
    now = DateTime.truncate(DateTime.utc_now(), :second)
    action = event[@crud]

    occurred_at =
      case event["_timestamp"] do
        nil ->
          nil

        date_string ->
          {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

          dt_struct
          |> DateTime.truncate(:second)
      end

    pg_event =
      case action do
        "update" ->
          %{
            core_id: core_id,
            occurred_at: occurred_at,
            risk_score: format_risk_score(event["_confidence"]),
            event_details: format_lexicon_data(event),
            event_definition_id: event_definition_id,
            created_at: now,
            updated_at: now
          }

        _ ->
          %{
            core_id: core_id,
            occurred_at: occurred_at,
            risk_score: format_risk_score(event["_confidence"]),
            event_details: format_lexicon_data(event),
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
  def process_elasticsearch_documents(%{crud_action: "delete"} = data), do: data

  def process_elasticsearch_documents(
        %{
          event: event,
          pg_event: pg_event,
          core_id: core_id,
          event_definition: event_definition,
          event_definition_id: event_definition_id
        } = data
      ) do
    published_at = event["published_at"]
    event_definition_details = event_definition.event_definition_details

    # Iterate over each event key value pair and build the pg and elastic search event
    # details.
    elasticsearch_event_details =
      Enum.reduce(pg_event.event_details, [], fn
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

    Map.put(data, :event_doc, elasticsearch_event_doc)
    |> Map.put(:pipeline_state, :process_elasticsearch_documents)
  end

  @doc """
  """
  def process_notifications(%{crud_action: "delete"} = data), do: data

  def process_notifications(
        %{
          pg_event: pg_event,
          core_id: core_id,
          event_definition: event_definition
        } = data
      ) do
    pg_notifications =
      NotificationsContext.fetch_valid_notification_settings(
        %{
          event_definition_id: event_definition.id,
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

    # pg_notifications_delete =
    #   NotificationsContext.fetch_invalid_notification_settings(
    #     %{
    #       event_definition_id: event_definition.id,
    #       active: true
    #     },
    #     pg_event.risk_score,
    #     event_definition
    #   )
    #   |> Enum.reduce([], fn ns, acc ->

    #     # TODO: query for notifications where core_id and notification_setting_id match
    #     # build a list of the notification_setting_ids to remove

    #     acc ++
    #       [
    #         %{
    #           archived_at: nil,
    #           priority: @defaults.notification_priority,
    #           assigned_to: ns.assigned_to,
    #           dismissed_at: nil,
    #           core_id: core_id,
    #           tag_id: ns.tag_id,
    #           notification_setting_id: ns.id,
    #           created_at: now,
    #           updated_at: now
    #         }
    #       ]
    #   end)

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
      pg_notifications: [],
      event_doc: [],
      pg_event_links: []
    }

    bulk_transactional_data =
      Enum.reduce(messages, default_map, fn data, acc ->
        data =
          Map.drop(data, [
            :crud_action,
            :event,
            :event_definition,
            :event_definition_id,
            :retry_count,
            :pipeline_state
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

            _ ->
              v2
          end
        end)
      end)

    # Elasticsearch Transactional Upserts
    bulk_upsert_event_documents_with_transaction(bulk_transactional_data)

    # IO.inspect(bulk_transactional_data.pg_event, label: "EVENTS")
    # IO.inspect(bulk_transactional_data.pg_notifications, label: "NOTIFICATIONS")
    # IO.inspect(bulk_transactional_data.pg_event_links, label: "EVENT LINKS")

    # Build a Multi transaction to insert all the pg records
    transaction_result =
      Multi.new()
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
      |> EventsContext.upsert_all_event_links_multi(bulk_transactional_data.pg_event_links,
        on_conflict: {:replace_all_except, [:id, :created_at]},
        conflict_target: [:link_core_id]
      )
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok, %{upsert_notifications: {_count_created, upserted_notifications}}} ->
        SystemNotificationContext.bulk_insert_system_notifications(upserted_notifications)

      # TODO: send Redis pub sub for Events object

      {:ok, _} ->
        nil

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
  defp bulk_upsert_event_documents_with_transaction(bulk_transactional_data) do
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

  defp rollback_event_index_data(bulk_transactional_data) do
    event_doc_ids =
      bulk_transactional_data.event_doc
      |> Enum.map(fn event_doc -> event_doc.id end)

    Elasticsearch.bulk_delete_document(
      Config.event_index_alias(),
      event_doc_ids
    )
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

  defp format_risk_score(nil), do: 0

  defp format_risk_score(risk_score) when is_float(risk_score),
    do: trunc(Float.round(risk_score * 100))

  defp format_risk_score(risk_score) when is_integer(risk_score), do: risk_score

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
