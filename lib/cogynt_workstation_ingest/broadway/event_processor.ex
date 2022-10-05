defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  # alias Ecto.Multi
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Config
  # alias CogyntWorkstationIngest.System.SystemNotificationContext
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
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    action = Map.get(event, Config.crud_key(), nil)

    cond do
      action == Config.crud_delete_value() ->
        Map.put(data, :crud_action, action)
        |> Map.put(:delete_core_id, core_id)
        |> Map.put(:pipeline_state, :process_event)

      true ->
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

        risk_score = format_risk_score(event[Config.confidence_key()])

        event_details = format_lexicon_data(event)

        pg_event_list = [
          "#{core_id}\t#{occurred_at}\t#{risk_score}\t#{Jason.encode!(event_details)}\t#{now}\t#{now}\t#{event_definition_hash_id}\n"
        ]

        pg_event_map = %{
          core_id: core_id,
          occurred_at: occurred_at,
          risk_score: risk_score,
          event_details: event_details,
          created_at: now,
          updated_at: now,
          event_definition_hash_id: event_definition_hash_id
        }

        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :process_event],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        Map.put(data, :pg_event_list, pg_event_list)
        |> Map.put(:pg_event_map, pg_event_map)
        |> Map.put(:crud_action, action)
        |> Map.put(:pipeline_state, :process_event)
    end
  end

  def process_event_history(
        %{event: event, event_definition_hash_id: event_definition_hash_id, core_id: core_id} =
          data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

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

    published_at =
      case event[Config.published_at_key()] do
        nil ->
          nil

        date_string ->
          {:ok, dt_struct, _utc_offset} = DateTime.from_iso8601(date_string)

          dt_struct
          |> DateTime.truncate(:second)
      end

    risk_score = format_risk_score(event[Config.confidence_key()])

    version = event[Config.version_key()]
    event_details = format_lexicon_data(event)

    cond do
      action == Config.crud_delete_value() ->
        pg_event_history = [
          "#{Ecto.UUID.generate()}\t#{core_id}\t#{event_definition_hash_id}\t#{action}\t#{risk_score}\t#{version}\t#{Jason.encode!(event_details)}\t#{occurred_at}\t#{published_at}\n"
        ]

        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :process_event_history],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        Map.put(data, :crud_action, action)
        |> Map.put(:pg_event_history, pg_event_history)
        |> Map.put(:delete_core_id, core_id)
        |> Map.put(:pipeline_state, :process_event_history)

      action == Config.crud_create_value() or action == Config.crud_update_value() ->
        pg_event_history = [
          "#{Ecto.UUID.generate()}\t#{core_id}\t#{event_definition_hash_id}\t#{action}\t#{risk_score}\t#{version}\t#{Jason.encode!(event_details)}\t#{occurred_at}\t#{published_at}\n"
        ]

        pg_event_list = [
          "#{core_id}\t#{occurred_at}\t#{risk_score}\t#{Jason.encode!(event_details)}\t#{now}\t#{now}\t#{event_definition_hash_id}\n"
        ]

        pg_event_map = %{
          core_id: core_id,
          occurred_at: occurred_at,
          risk_score: risk_score,
          event_details: event_details,
          created_at: now,
          updated_at: now,
          event_definition_hash_id: event_definition_hash_id
        }

        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :process_event_history],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        Map.put(data, :pg_event_list, pg_event_list)
        |> Map.put(:pg_event_map, pg_event_map)
        |> Map.put(:pg_event_history, pg_event_history)
        |> Map.put(:crud_action, action)
        |> Map.put(:pipeline_state, :process_event_history)

      true ->
        pg_event_list = [
          "#{core_id}\t#{occurred_at}\t#{risk_score}\t#{Jason.encode!(event_details)}\t#{now}\t#{now}\t#{event_definition_hash_id}\n"
        ]

        pg_event_map = %{
          core_id: core_id,
          occurred_at: occurred_at,
          risk_score: risk_score,
          event_details: event_details,
          created_at: now,
          updated_at: now,
          event_definition_hash_id: event_definition_hash_id
        }

        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :process_event_history],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        Map.put(data, :pg_event_list, pg_event_list)
        |> Map.put(:pg_event_map, pg_event_map)
        |> Map.put(:crud_action, action)
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
          pg_event_map: pg_event_map,
          core_id: core_id,
          event_definition: event_definition,
          event_definition_hash_id: event_definition_hash_id,
          event_type: event_type,
          crud_action: action,
          elastic_event_links: elastic_event_links
        } = data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    cond do
      action == Config.crud_delete_value() ->
        data

      true ->
        event_definition_details = event_definition.event_definition_details

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
                 title: event_definition.title,
                 event_definition_id: event_definition.event_definition_id,
                 event_definition_hash_id: event_definition_hash_id,
                 event_details: elasticsearch_event_details,
                 core_event_id: core_id,
                 event_type: event_type,
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

  def process_notifications(
        %{
          pg_event_map: pg_event_map,
          core_id: core_id,
          event_definition: event_definition,
          crud_action: action
        } = data
      ) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    cond do
      action == Config.crud_delete_value() ->
        Map.put(data, :pipeline_state, :process_notifications)

      true ->
        case NotificationsContext.query_notification_settings(%{
               filter: %{
                 event_definition_hash_id: event_definition.id
               }
             }) do
          [] ->
            # Execute telemtry for metrics
            :telemetry.execute(
              [:broadway, :process_notifications],
              %{duration: System.monotonic_time() - start},
              telemetry_metadata
            )

            Map.put(data, :pipeline_state, :process_notifications)

          _ ->
            pg_notifications =
              NotificationsContext.fetch_valid_notification_settings(
                %{
                  event_definition_hash_id: event_definition.id,
                  active: true
                },
                pg_event_map.risk_score,
                event_definition
              )
              |> Enum.reduce([], fn ns, acc ->
                now = DateTime.truncate(DateTime.utc_now(), :second)

                acc ++
                  [
                    "#{Ecto.UUID.generate()}\t#{core_id}\t#{nil}\t#{@defaults.notification_priority}\t#{ns.assigned_to}\t#{nil}\t#{ns.id}\t#{ns.tag_id}\t#{now}\t#{now}\n"
                  ]
              end)

            invalid_notification_settings =
              NotificationsContext.fetch_invalid_notification_settings(
                %{
                  event_definition_hash_id: event_definition.id,
                  active: true
                },
                pg_event_map.risk_score,
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

            # Execute telemtry for metrics
            :telemetry.execute(
              [:broadway, :process_notifications],
              %{duration: System.monotonic_time() - start},
              telemetry_metadata
            )

            Map.put(data, :pg_notifications, pg_notifications)
            |> Map.put(:pg_notifications_delete, pg_notifications_delete)
            |> Map.put(:pipeline_state, :process_notifications)
        end
    end
  end

  @doc """
  Takes all the messages that have gone through the processing steps in the pipeline up to the batch limit
  configured. Will execute one multi transaction to delete and upsert all objects
  """
  def execute_batch_transaction(messages, event_type, pg_event_history \\ []) do
    # build transactional data
    default_map = %{
      pg_event_list: [],
      pg_notifications: [],
      event_doc: [],
      pg_event_map: [],
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
            :elastic_event_links,
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

            :pg_event_map ->
              v1 ++ [v2]

            :pg_event_list ->
              v1 ++ v2

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
    end

    nil
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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
