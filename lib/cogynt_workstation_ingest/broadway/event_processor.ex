defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Elasticsearch.ElasticApi
  alias CogyntWorkstationIngest.Elasticsearch.EventDocumentBuilder
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  @defaults %{
    crud_action: nil,
    event_document: nil,
    notification_priority: 3
  }

  @doc """
  process_event/1 for a CRUD: delete event we just store the core_id of the event that needs
  to be deleted as part of the payload. When it hits the transactional step of the pipeline the
  event and its associated data will be removed, otherwise it will build an event map and SQL upsert
  that will be processed in bulk in the transactional step of the pipeline
  """
  def process_event(
        %{core_id: core_id, event_definition: %{id: event_definition_hash_id}, kafka_event: event} =
          data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}
    action = Map.get(event, String.to_atom(Config.crud_key()), nil)

    if action == Config.crud_delete_value() do
      data
      |> Map.put(:pipeline_state, :process_event)
      |> Map.put(:crud_action, action)
      |> Map.put(:delete_core_id, core_id)
    else
      now = DateTime.truncate(DateTime.utc_now(), :second)

      risk_score =
        format_risk_score(get_in(data, [:kafka_event, String.to_atom(Config.confidence_key())]))

      event_details = format_lexicon_data(Map.get(data, :kafka_event))

      occurred_at =
        case get_in(data, [:kafka_event, String.to_atom(Config.timestamp_key())]) do
          nil ->
            nil

          date_string ->
            {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

            dt_struct
            |> DateTime.truncate(:second)
        end

      # Execute telemtry for metrics
      :telemetry.execute(
        [:broadway, :process_event],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

      data
      |> Map.put(:pipeline_state, :process_event)
      |> Map.put(:crud_action, action)
      |> Map.put(
        :pg_event_list,
        "#{core_id}\t#{occurred_at}\t#{risk_score}\t#{Jason.encode!(event_details)}\t#{now}\t#{now}\t#{event_definition_hash_id}\n"
      )
      |> Map.put(:pg_event_map, %{
        core_id: core_id,
        occurred_at: occurred_at,
        risk_score: risk_score,
        event_details: event_details,
        created_at: now,
        updated_at: now,
        event_definition_hash_id: event_definition_hash_id
      })
    end
  end

  def process_event_history(
        %{
          core_id: core_id,
          crud_action: action,
          event_definition: %{id: event_definition_hash_id},
          kafka_event: k_event
        } = data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}
    crud_delete_value = Config.crud_delete_value()
    crud_create_value = Config.crud_create_value()
    crud_update_value = Config.crud_update_value()

    # If we have a crud action we need to add an entry for pg_event_history
    if action in [crud_create_value, crud_update_value, crud_delete_value] do
      event_details = format_lexicon_data(k_event)
      risk_score = format_risk_score(Map.get(k_event, String.to_atom(Config.confidence_key())))
      version = Map.get(k_event, String.to_atom(Config.version_key()))

      occurred_at =
        case Map.get(k_event, String.to_atom(Config.timestamp_key())) do
          nil ->
            nil

          date_string ->
            {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

            dt_struct
            |> DateTime.truncate(:second)
        end

      published_at =
        case Map.get(k_event, String.to_atom(Config.published_at_key())) do
          nil ->
            nil

          date_string ->
            {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

            dt_struct
            |> DateTime.truncate(:second)
        end

      # Execute telemtry for metrics
      :telemetry.execute(
        [:broadway, :process_event_history],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

      data
      |> Map.put(:pipeline_state, :process_event_history)
      |> Map.put(
        :pg_event_history,
        "#{Ecto.UUID.generate()}\t#{core_id}\t#{event_definition_hash_id}\t#{action}\t#{risk_score}\t#{version}\t#{Jason.encode!(event_details)}\t#{occurred_at}\t#{published_at}\n"
      )

      # If there is no crud action we ignore pg_event_history.
    else
      data
      |> Map.put(:pipeline_state, :process_event_history)
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
          core_id: core_id,
          crud_action: action,
          event_definition: %{
            id: event_definition_hash_id,
            event_definition_id: event_definition_id,
            title: title,
            event_type: event_type,
            event_definition_details: event_definition_details
          },
          elastic_event_links: elastic_event_links,
          pg_event_map: pg_event_map
        } = data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}
    crud_delete_value = Config.crud_delete_value()

    if action == crud_delete_value do
      # Execute telemetry for metrics
      :telemetry.execute(
        [:broadway, :process_elasticsearch_documents],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

      data
      |> Map.put(:pipeline_state, :process_elasticsearch_documents)
    else
      # Iterate over each event key value pair and build the pg and elastic search event
      # details.
      elasticsearch_event_details =
        Enum.filter(
          event_definition_details,
          &(not String.starts_with?(Map.get(&1, :field_name), "COG_"))
        )
        |> Stream.unfold(fn
          [] -> nil
          [detail] -> {determine_event_detail(detail, pg_event_map.event_details), []}
          [detail | tail] -> {determine_event_detail(detail, pg_event_map.event_details), tail}
        end)
        |> Enum.to_list()

      # Build elasticsearch documents
      elasticsearch_event_doc =
        case EventDocumentBuilder.build_document(%{
               id: core_id,
               title: title,
               event_definition_id: event_definition_id,
               event_definition_hash_id: event_definition_hash_id,
               event_details: elasticsearch_event_details,
               core_event_id: core_id,
               event_type: Atom.to_string(event_type),
               occurred_at: pg_event_map.occurred_at,
               risk_score: pg_event_map.risk_score,
               event_links: elastic_event_links
             }) do
          {:ok, event_doc} ->
            event_doc

          _ ->
            @defaults.event_document
        end

      # Execute telemtry for metrics
      :telemetry.execute(
        [:broadway, :process_elasticsearch_documents],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

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

  def process_notifications(%{event_definition: %{notification_settings: []}} = data) do
    data
    |> Map.put(:pipeline_state, :process_notifications)
  end

  def process_notifications(
        %{
          pg_event_map: pg_event_map,
          core_id: core_id,
          event_definition: %{
            notification_settings: notification_settings,
            event_definition_details: event_definition_details
          },
          crud_action: action
        } = data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    if action == Config.crud_delete_value() do
      Map.put(data, :pipeline_state, :process_notifications)
    else
      grouped_notification_settings =
        notification_settings
        |> Enum.group_by(fn
          ns ->
            if ns.active and in_risk_range?(pg_event_map.risk_score, ns.risk_range) and
                 Enum.any?(event_definition_details, &(Map.get(&1, :path) == ns.title)) do
              :upsert
            else
              :delete
            end
        end)

      pg_notifications =
        Map.get(grouped_notification_settings, :upsert, [])
        |> Enum.map(fn ns ->
          now = DateTime.truncate(DateTime.utc_now(), :second)

          "#{Ecto.UUID.generate()}\t#{core_id}\t#{nil}\t#{@defaults.notification_priority}\t#{ns.assigned_to}\t#{nil}\t#{ns.id}\t#{ns.tag_id}\t#{now}\t#{now}\n"
        end)

      pg_notifications_delete =
        if is_nil(Map.get(grouped_notification_settings, :delete)) do
          []
        else
          # TODO: HOW CAN WE MOVE THIS TO A SINGLE QUERY FOR ALL MESSAGES
          # Also should ingest be responsible for the deletion
          # of notifications? If a notification has changed the notification
          # should be deleted by the notification worker if it no
          # longer meets the criteria?
          NotificationsContext.query_notifications(%{
            filter: %{
              notification_setting_ids:
                Enum.map(Map.get(grouped_notification_settings, :delete), &Map.get(&1, :id)),
              core_id: core_id
            }
          })
          |> Enum.map(fn notifications -> notifications.core_id end)
        end

      # Execute telemtry for metrics
      :telemetry.execute(
        [:broadway, :process_notifications],
        %{duration: System.monotonic_time() - start},
        telemetry_metadata
      )

      data
      |> Map.put(:pg_notifications, pg_notifications)
      |> Map.put(:pg_notifications_delete, pg_notifications_delete)
      |> Map.put(:pipeline_state, :process_notifications)
    end
  end

  @doc """
  Takes all the messages that have gone through the processing steps in the pipeline up to the batch limit
  configured. Will execute one multi transaction to delete and upsert all objects
  """
  def execute_batch_transaction(messages, is_crud?, event_type) do
    # build transactional data
    default_map = %{
      pg_event_list: [],
      pg_notifications: [],
      event_doc: [],
      pg_event_map: [],
      pg_event_links: [],
      delete_core_id: [],
      pg_notifications_delete: [],
      pg_event_links_delete: [],
      pg_event_history:
        if(is_crud?, do: Enum.map(messages, &Map.get(&1, :pg_event_history)), else: [])
    }

    bulk_transactional_data =
      if(is_crud?, do: filter_messages_by_core_id(messages), else: messages)
      |> Enum.reduce(default_map, fn
        data, default_map ->
          default_map
          |> Map.update!(
            :event_doc,
            &if(Map.get(data, :event_doc) != @defaults.event_document,
              do: &1 ++ [Map.get(data, :event_doc)],
              else: &1
            )
          )
          |> Map.update!(
            :pg_event_map,
            &if(Map.get(data, :pg_event_map), do: &1 ++ [Map.get(data, :pg_event_map)], else: &1)
          )
          |> Map.update!(
            :pg_event_list,
            &if(Map.get(data, :pg_event_list), do: &1 ++ [Map.get(data, :pg_event_list)], else: &1)
          )
          |> Map.update!(
            :pg_notifications,
            &if(Map.get(data, :pg_notifications),
              do: &1 ++ Map.get(data, :pg_notifications),
              else: &1
            )
          )
          |> Map.update!(
            :pg_event_links,
            &if(Map.get(data, :pg_event_links), do: &1 ++ Map.get(data, :pg_event_links), else: &1)
          )
          |> Map.update!(
            :delete_core_id,
            &if(Map.get(data, :delete_core_id),
              do: &1 ++ [Map.get(data, :delete_core_id)],
              else: &1
            )
          )
          |> Map.update!(
            :pg_notifications_delete,
            &if(Map.get(data, :pg_notifications_delete),
              do: &1 ++ Map.get(data, :pg_notifications_delete),
              else: &1
            )
          )
          |> Map.update!(
            :pg_event_links_delete,
            &if(Map.get(data, :pg_event_links_delete),
              do: &1 ++ Map.get(data, :pg_event_links_delete),
              else: &1
            )
          )
      end)

    case bulk_upsert_event_documents(bulk_transactional_data) do
      {:ok, :success} ->
        case EventsContext.execute_ingest_bulk_insert_function(bulk_transactional_data) do
          {:ok, _} ->
            bulk_delete_event_documents(bulk_transactional_data)

          Redis.publish_async(
            "events_changed_listener",
            %{
              event_type: event_type,
              deleted: bulk_transactional_data.delete_core_id,
              upserted: bulk_transactional_data.pg_event_map
            }
          )

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            rollback_event_index_data(bulk_transactional_data)

            raise "execute_transaction/1 failed"
        end

      {:error, :failed} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: bulk_upsert_event_documents failed"
        )

        raise "execute_transaction/1 failed"

      {:error, error_ids} when is_list(error_ids) ->
        bulk_transactional_data
        |> filter_failed_pg_events(error_ids)
        |> filter_failed_notifications(error_ids)
        |> filter_failed_event_history(error_ids)
        |> filter_failed_event_links(error_ids)
        |> EventsContext.execute_ingest_bulk_insert_function()
        |> case do
          {:ok, _} ->
            bulk_delete_event_documents(bulk_transactional_data)

          Redis.publish_async(
            "events_changed_listener",
            %{
              event_type: event_type,
              deleted: bulk_transactional_data.delete_core_id,
              upserted: bulk_transactional_data.pg_event_map
            }
          )

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            rollback_event_index_data(bulk_transactional_data)

            raise "execute_transaction/1 failed"
        end
    end

    nil
  end

  defp determine_event_detail(
         %{path: path, field_name: field_name, field_type: "geo"},
         event_details
       ) do
    get_in(event_details, String.split(path, "|"))
    |> case do
      %{"type" => "Feature", "geometry" => g} ->
        %{field_name: field_name, field_type: "geo", path: path} |> Map.put("geo", g)

      value ->
        %{field_name: field_name, field_type: "geo", path: path} |> Map.put("geo", value)
    end
  end

  defp determine_event_detail(
         %{path: path, field_name: field_name, field_type: "array"},
         event_details
       ) do
    get_in(event_details, String.split(path, "|"))
    |> case do
      value when is_list(value) ->
        Jason.encode(value)
        |> case do
          {:ok, value} ->
            %{field_name: field_name, field_type: "array", path: path} |> Map.put("array", value)

          _ ->
            %{field_name: field_name, field_type: "array", path: path}
            |> Map.put("array", inspect(value))
        end

      value ->
        %{field_name: field_name, field_type: "array", path: path}
        |> Map.put("array", inspect(value))
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp determine_event_detail(
         %{path: path, field_name: field_name, field_type: field_type},
         event_details
       ) do
    if(field_type == "array", do: get_in(event_details, String.split(path, "|")))

    %{field_name: field_name, field_type: field_type, path: path}
    |> Map.put(field_type, get_in(event_details, String.split(path, "|")))
  end

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
        {:ok, %{"errors" => true, "items" => items}} ->
          error_ids =
            Enum.group_by(
              items,
              fn
                %{"update" => event} -> get_in(event, ["error", "type"])
                %{"create" => event} -> get_in(event, ["error", "type"])
                # We shouldn't hit this since we are doing an upsert but I don't want
                # this to cause an exception
                _ -> nil
              end,
              fn
                %{"update" => event} -> get_in(event, ["_id"])
                %{"create" => event} -> get_in(event, ["_id"])
                # We shouldn't hit this since we are doing an upsert but I don't want
                # this to cause an exception
                _ -> nil
              end
            )
            |> Map.drop([nil])

          # Log the errors
          Enum.each(error_ids, fn
            {reason, events} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "Failed to upsert #{length(events)} events into Elasticsearch for the following reason \"#{reason}\""
              )
          end)

          {:error, Map.values(error_ids) |> List.flatten()}

        {:ok, %{"errors" => false}} ->
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

  defp filter_messages_by_core_id(messages) do
    messages
    |> Enum.group_by(fn data -> data.core_id end)
    |> Enum.reduce([], fn {_core_id, core_id_records}, acc ->
      # We only need to process the last action that occurred for the
      # core_id within the batch of events that were sent to handle_batch
      # ex: create, update, update, update, delete, create (only need the last create)
      last_crud_action_message = List.last(core_id_records)

      acc ++ [last_crud_action_message]
    end)
  end

  defp format_lexicon_data(event) do
    matches = String.to_atom(Config.matches_key())
    case Map.get(event, matches) do
      nil ->
        event

      lexicon_val ->
        try do
          Map.put(
            event,
            matches,
            List.flatten(lexicon_val) |> Enum.filter(&is_binary(&1))
          )
        rescue
          _ ->
            CogyntLogger.error("#{__MODULE__}", "Lexicon value incorrect format #{lexicon_val}")
            Map.delete(event, matches)
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

  defp filter_failed_event_links(%{pg_event_links: links} = bulk_transactional_data, error_ids) do
    filtered_links =
      links
      |> Enum.reject(fn
        <<core_id::binary-size(36), "\t"::binary, enitity_core_id::binary-size(36),
          _rest::binary>> ->
          core_id in error_ids or enitity_core_id in error_ids

        _ ->
          false
      end)

    Map.put(bulk_transactional_data, :pg_event_links, filtered_links)
  end

  defp filter_failed_event_links(bulk_transactional_data, _), do: bulk_transactional_data

  defp filter_failed_event_history(
         %{pg_event_history: history_list} = bulk_transactional_data,
         error_ids
       ) do
    filtered_history =
      history_list
      |> Enum.reject(fn
        <<_id::binary-size(36), "\t"::binary, core_id::binary-size(36), _rest::binary>> ->
          core_id in error_ids

        _ ->
          false
      end)

    Map.put(bulk_transactional_data, :pg_event_history, filtered_history)
  end

  defp filter_failed_event_history(bulk_transactional_data, _), do: bulk_transactional_data

  defp filter_failed_notifications(
         %{pg_notifications: notification_list} = bulk_transactional_data,
         error_ids
       ) do
    filtered_notifications =
      notification_list
      |> Enum.reject(fn
        <<_id::binary-size(36), "\t"::binary, core_id::binary-size(36), _rest::binary>> ->
          core_id in error_ids

        _ ->
          false
      end)

    Map.put(bulk_transactional_data, :pg_notifications, filtered_notifications)
  end

  defp filter_failed_notifications(bulk_transactional_data, _), do: bulk_transactional_data

  defp filter_failed_pg_events(%{pg_event_list: event_list} = bulk_transactional_data, error_ids) do
    filtered_events =
      event_list
      |> Enum.reject(fn
        <<core_id::binary-size(36), _rest::binary>> -> core_id in error_ids
        _ -> false
      end)

    Map.put(bulk_transactional_data, :pg_event_list, filtered_events)
  end

  defp filter_failed_pg_events(bulk_transactional_data, _), do: bulk_transactional_data

  defp in_risk_range?(risk_score, risk_range) do
    risk_score = risk_score || 0
    risk_score >= Enum.min(risk_range) and risk_score <= Enum.max(risk_range)
  end
end
