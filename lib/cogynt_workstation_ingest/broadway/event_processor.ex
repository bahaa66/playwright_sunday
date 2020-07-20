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
  @partial Application.get_env(:cogynt_workstation_ingest, :core_keys)[:partial]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]
  @lexicons Application.get_env(:cogynt_workstation_ingest, :core_keys)[:lexicons]
  @elastic_blacklist [@entities, @crud, @partial, @risk_score]

  @doc """
  Requires event field in the data map. Based on the crud action value
  process_event/1 will create a single Event record in the database that is assosciated with
  the event_definition.id. It will also pull all the event_ids and doc_ids that need to be
  soft_deleted from the database and elasticsearch. The data map is updated with the :event_id,
  :delete_event_ids, :delete_docs fields.
  """
  def process_event(%{event: %{@crud => action}, event_id: nil} = data) do
    {:ok,
     %{
       event_id: new_event_id,
       delete_event_ids: delete_event_ids,
       delete_doc_ids: delete_doc_ids
     }} =
      case action do
        @delete ->
          delete_event(data)

        _ ->
          create_or_update_event(data)
      end

    Map.put(data, :event_id, new_event_id)
    |> Map.put(:delete_event_ids, delete_event_ids)
    |> Map.put(:delete_docs, delete_doc_ids)
    |> Map.put(:crud_action, action)
  end

  def process_event(%{event: %{@crud => action}, event_id: event_id} = data) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "deleting event for retry message: #{event_id}"
    )

    EventsContext.soft_delete_events([event_id])

    {:ok,
     %{
       event_id: new_event_id,
       delete_event_ids: delete_event_ids,
       delete_doc_ids: delete_doc_ids
     }} =
      case action do
        @delete ->
          delete_event(data)

        _ ->
          create_or_update_event(data)
      end

    Map.put(data, :event_id, new_event_id)
    |> Map.put(:delete_event_ids, delete_event_ids)
    |> Map.put(:delete_docs, delete_doc_ids)
    |> Map.put(:crud_action, action)
  end

  @doc """
  Requires event_definition field in the data map. process_event/1 will create a single Event
  record in the database that is assosciated with the event_definition.id. The data map
  is updated with the :event_id returned from the database.
  """
  def process_event(%{event: event, event_definition: event_definition, event_id: nil} = data) do
    case EventsContext.create_event(%{
           event_definition_id: event_definition.id,
           core_id: event["id"]
         }) do
      {:ok, %{id: event_id}} ->
        Map.put(data, :event_id, event_id)
        |> Map.put(:delete_event_ids, nil)
        |> Map.put(:delete_docs, nil)
        |> Map.put(:crud_action, nil)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "process_event/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "process_event/1 failed"
    end
  end

  def process_event(
        %{event: event, event_definition: event_definition, event_id: event_id} = data
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "deleting event for retry message: #{event_id}"
    )

    EventsContext.soft_delete_events([event_id])

    case EventsContext.create_event(%{
           event_definition_id: event_definition.id,
           core_id: event["id"]
         }) do
      {:ok, %{id: event_id}} ->
        Map.put(data, :event_id, event_id)
        |> Map.put(:delete_event_ids, nil)
        |> Map.put(:delete_docs, nil)
        |> Map.put(:crud_action, nil)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "process_event/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "process_event/1 failed"
    end
  end

  @doc """
  Requires event, event_definition and event_id fields in the data map. Takes the
  field_name and field_value fields from the event and creates a list of event_detail
  maps. Also creates a list of elasticsearch docs. Returns an updated data map with
  the :event_details, :risk_history_doc and :event_docs values.
  """
  def process_event_details_and_elasticsearch_docs(%{event_id: nil} = data), do: data

  def process_event_details_and_elasticsearch_docs(
        %{event: event, event_definition: event_definition, event_id: event_id} = data
      ) do
    action = Map.get(event, @crud)
    event = format_lexicon_data(event)

    {event_details, event_docs} =
      Enum.reduce(event, {[], []}, fn {field_name, field_value}, {acc_events, acc_docs} = acc ->
        field_type = event_definition.fields[field_name]

        case is_nil(field_value) or field_value == "" do
          false ->
            field_value = encode_json(field_value)

            # Build event_details list
            updated_events =
              acc_events ++
                [
                  %{
                    event_id: event_id,
                    field_name: field_name,
                    field_type: field_type,
                    field_value: field_value
                  }
                ]

            # Build elasticsearch docs list
            # If event is delete action or field_name is in blacklist we do not need to insert into elasticƒ
            updated_docs =
              if Enum.member?(@elastic_blacklist, field_name) == false and action != @delete do
                acc_docs ++
                  [
                    EventDocumentBuilder.build_document(
                      event,
                      field_name,
                      field_value,
                      event_definition,
                      event_id,
                      action
                    )
                  ]
              else
                acc_docs
              end

            {updated_events, updated_docs}

          true ->
            acc
        end
      end)

    Map.put(data, :event_details, event_details)
    |> Map.put(:event_docs, event_docs)
    |> Map.put(:risk_history_doc, RiskHistoryDocumentBuilder.build_document(event_id, event))
  end

  @doc """
  Requires event, event_definition and event_id fields in the data map. process_notifications/1
  will stream all notification_settings that are linked to the event_definition.id. On each
  notification_setting returned it will build a notification map. Finally it will return a list
  notification maps. Returns an updated data map with the field :notifications storing the list
  of notification maps.
  """
  def process_notifications(%{event_id: nil} = data), do: data

  def process_notifications(
        %{
          event: event,
          event_definition: event_definition,
          event_id: event_id,
          delete_event_ids: nil
        } = data
      ) do
    case NotificationsContext.process_notifications(%{
           event_definition: event_definition,
           event_id: event_id,
           risk_score: Map.get(event, @risk_score, 0)
         }) do
      {:ok, nil} ->
        data

      {:ok, notifications} ->
        Map.put(data, :notifications, notifications)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "process_notifications/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "process_notifications/1 failed"
    end
  end

  def process_notifications(
        %{
          event: _event,
          event_definition: _event_definition,
          event_id: _event_id,
          delete_event_ids: _delete_event_ids
        } = data
      ),
      do: data

  @doc """
  Requires :event_details, :notifications, :event_docs, :risk_history_doc, :delete_event_ids, and :delete_docs
  fields in the data map. Takes all the fields and executes them in one databse transaction. When
  it finishes with no errors it will update the :event_processed key to have a value of true
  in the data map and return.
  """
  def execute_transaction(%{event_id: nil} = data), do: data

  def execute_transaction(
        %{
          notifications: notifications,
          event_details: event_details,
          delete_event_ids: delete_event_ids,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_docs: doc_ids,
          crud_action: action,
          event_id: event_id
        } = data
      ) do
    # elasticsearch updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    transaction_result =
      EventsContext.insert_all_event_details_multi(event_details)
      |> NotificationsContext.insert_all_notifications_multi(notifications,
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
      |> EventsContext.update_all_events_multi(delete_event_ids)
      |> NotificationsContext.update_all_notifications_multi(%{
        delete_event_ids: delete_event_ids,
        action: action,
        event_id: event_id
      })
      |> EventsContext.update_all_event_links_multi(delete_event_ids)
      |> SystemNotificationContext.insert_all_multi()
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok,
       %{
         insert_notifications: {_count_created, created_notifications},
         update_notifications: {_count_deleted, updated_notifications}
       }} ->
        total_notifications =
          NotificationsContext.notification_struct_to_map(created_notifications) ++
            updated_notifications

        Redis.publish_async("notification_count_subscription", total_notifications)

      {:ok, %{insert_notifications: {_count_created, created_notifications}}} ->
        Redis.publish_async(
          "notification_count_subscription",
          NotificationsContext.notification_struct_to_map(created_notifications)
        )

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "execute_transaction/1 failed"
    end

    data
  end

  def execute_transaction(
        %{
          event_details: event_details,
          delete_event_ids: delete_event_ids,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_docs: doc_ids,
          crud_action: action,
          event_id: event_id
        } = data
      ) do
    # elasticsearch updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    transaction_result =
      EventsContext.insert_all_event_details_multi(event_details)
      |> EventsContext.update_all_events_multi(delete_event_ids)
      |> NotificationsContext.update_all_notifications_multi(%{
        delete_event_ids: delete_event_ids,
        action: action,
        event_id: event_id
      })
      |> EventsContext.update_all_event_links_multi(delete_event_ids)
      |> SystemNotificationContext.update_all_multi()
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok, %{update_notifications: {_count, updated_notifications}}} ->
        Redis.publish_async("notification_count_subscription", updated_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "execute_transaction/1 failed"
    end

    data
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
          }",
          true
        )

        {:error, {nil, nil}}

      core_id ->
        delete_event_ids = EventsContext.get_events_by_core_id(core_id, event_definition.id)
        delete_doc_ids = EventDocumentBuilder.build_document_ids(core_id, event_definition)
        {:ok, {delete_event_ids, delete_doc_ids}}
    end
  end

  defp delete_event(%{event: event, event_definition: event_definition} = data) do
    # Delete event -> get all data to remove + create a new event
    # append new event to the list of data to remove
    case fetch_data_to_delete(data) do
      {:error, {nil, nil}} ->
        # will skip all steps in the pipeline ignoring this record
        {:ok,
         %{
           event_id: nil,
           delete_event_ids: nil,
           delete_doc_ids: nil
         }}

      {:ok, {nil, delete_doc_ids}} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: [event_id],
               delete_doc_ids: delete_doc_ids
             }}

          {:error, reason} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "delete_event/1 failed with reason: #{inspect(reason, pretty: true)}"
            )

            raise "delete_event/1 failed"
        end

      {:ok, {delete_event_ids, delete_doc_ids}} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: delete_event_ids ++ [event_id],
               delete_doc_ids: delete_doc_ids
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
      {:error, {nil, nil}} ->
        # will skip all steps in the pipeline ignoring this record
        {:ok,
         %{
           event_id: nil,
           delete_event_ids: nil,
           delete_doc_ids: nil
         }}

      {:ok, {delete_event_ids, _delete_doc_ids}} ->
        case EventsContext.create_event(%{
               event_definition_id: event_definition.id,
               core_id: event["id"]
             }) do
          {:ok, %{id: event_id}} ->
            {:ok,
             %{
               event_id: event_id,
               delete_event_ids: delete_event_ids,
               delete_doc_ids: nil
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

  defp update_event_docs(new_event_docs, delete_event_doc_ids) do
    if !is_nil(delete_event_doc_ids) and !Enum.empty?(delete_event_doc_ids) do
      {:ok, _} =
        Elasticsearch.bulk_delete_document(Config.event_index_alias(), delete_event_doc_ids)
    end

    if !is_nil(new_event_docs) and !Enum.empty?(new_event_docs) do
      {:ok, _} = Elasticsearch.bulk_upsert_document(Config.event_index_alias(), new_event_docs)
    end
  end

  defp update_risk_history_doc(risk_history_doc) do
    case !is_nil(risk_history_doc) do
      true ->
        {:ok, _} =
          Elasticsearch.upsert_document(
            Config.risk_history_index_alias(),
            risk_history_doc.id,
            risk_history_doc
          )

      false ->
        :ok
    end
  end
end
