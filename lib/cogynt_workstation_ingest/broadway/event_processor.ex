defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Elasticsearch.DocumentBuilders.{EventDocumentBuilder, RiskHistoryDocumentBuilder}
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Config

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @partial Application.get_env(:cogynt_workstation_ingest, :core_keys)[:partial]
  @update Application.get_env(:cogynt_workstation_ingest, :core_keys)[:update]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]
  @lexicons Application.get_env(:cogynt_workstation_ingest, :core_keys)[:lexicons]
  @elastic_blacklist [@entities, @crud, @partial, @risk_score]

  @doc """
  Requires event field in the data map. Based on the crud action value
  process_event/1 will create a single Event record in the database that is assosciated with
  the event_definition.id. It will also pull all the event_ids and doc_ids that need to be
  soft_deleted from the database and elasticsearch. The data map is updated with the :event_id,
  :delete_ids, :delete_docs fields.
  """
  def process_event(%{event: %{@crud => action}} = data) do
    case action do
      @update ->
        {:ok, {new_event_id, delete_event_ids, delete_doc_ids}} = update_event(data)

        Map.put(data, :event_id, new_event_id)
        |> Map.put(:delete_ids, delete_event_ids)
        |> Map.put(:delete_docs, delete_doc_ids)

      @delete ->
        {:ok, {new_event_id, delete_event_ids, delete_doc_ids}} = delete_event(data)

        Map.put(data, :event_id, new_event_id)
        |> Map.put(:delete_ids, delete_event_ids)
        |> Map.put(:delete_docs, delete_doc_ids)

      _ ->
        {:ok, {new_event_id, delete_event_ids, delete_doc_ids}} = create_event(data)

        Map.put(data, :event_id, new_event_id)
        |> Map.put(:delete_ids, delete_event_ids)
        |> Map.put(:delete_docs, delete_doc_ids)
    end
  end

  @doc """
  Requires event_definition field in the data map. process_event/1 will create a single Event
  record in the database that is assosciated with the event_definition.id. The data map
  is updated with the :event_id returned from the database.
  """
  def process_event(%{event_definition: event_definition} = data) do
    case EventsContext.create_event(%{event_definition_id: event_definition.id}) do
      {:ok, %{id: event_id}} ->
        Map.put(data, :event_id, event_id)
        |> Map.put(:delete_ids, nil)
        |> Map.put(:delete_docs, nil)

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "process_event/1 failed with reason: #{inspect(reason)}"
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
    # event = Map.drop(event, [@crud, @partial])

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
        %{event: event, event_definition: event_definition, event_id: event_id} = data
      ) do
    case publish_notification?(event) do
      true ->
        case NotificationsContext.process_notifications(%{
               event_definition: event_definition,
               event_id: event_id
             }) do
          {:ok, notifications} ->
            Map.put(data, :notifications, notifications)

          {:error, reason} ->
            CogyntLogger.error(
              "Event Processor",
              "process_notifications/1 failed with reason: #{inspect(reason)}"
            )

            raise "process_notifications/1 failed"
        end

      false ->
        Map.put(data, :notifications, nil)
    end
  end

  @doc """
  Requires :event_details, :notifications, :event_docs, :risk_history_doc, :delete_ids, and :delete_docs
  fields in the data map. Takes all the fields and executes them in one databse transaction. When
  it finishes with no errors it will update the :event_processed key to have a value of true
  in the data map and return.
  """
  def execute_transaction(%{event_id: nil} = data), do: Map.put(data, :event_processed, true)

  def execute_transaction(
        %{
          event_details: _event_details,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          notifications: nil,
          delete_ids: _event_ids,
          delete_docs: doc_ids
        } = data
      ) do
    case EventsContext.execute_event_processor_transaction(data) do
      {:ok, %{update_notifications: {_count, deleted_notifications}}} ->
        CogyntClient.publish_deleted_notifications(deleted_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "execute_transaction/1 failed with reason: #{inspect(reason)}"
        )

        raise "execute_transaction/1 failed"
    end

    # elastic search updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    Map.put(data, :event_processed, true)
  end

  def execute_transaction(
        %{
          event_details: _event_details,
          notifications: _notifications,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_ids: _event_ids,
          delete_docs: doc_ids
        } = data
      ) do
    case EventsContext.execute_event_processor_transaction(data) do
      {:ok,
       %{
         insert_notifications: {_count_created, created_notifications},
         update_notifications: {_count_deleted, deleted_notifications}
       }} ->
        CogyntClient.publish_deleted_notifications(deleted_notifications)
        CogyntClient.publish_notifications(created_notifications)

      {:ok, %{insert_notifications: {_count_created, created_notifications}}} ->
        CogyntClient.publish_notifications(created_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "execute_transaction/1 failed with reason: #{inspect(reason)}"
        )

        raise "execute_transaction/1 failed"
    end

    # elastic search updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    Map.put(data, :event_processed, true)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp format_lexicon_data(event) do
    case Map.get(event, @lexicons) do
      nil ->
        event

      val ->
        try do
          Map.put(event, @lexicons, List.flatten(val))
        rescue
          _ ->
            CogyntLogger.error("Invalid Authoring Data", "Lexicon value incorrect format #{val}")
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

  defp publish_notification?(event) do
    partial = Map.get(event, @partial)
    risk_score = Map.get(event, @risk_score)

    if partial == nil or partial == false or (risk_score != nil and risk_score > 0) do
      true
    else
      false
    end
  end

  defp fetch_data_to_delete(%{
         event: %{"id" => id},
         event_definition: event_definition
       }) do
    event_ids = EventsContext.fetch_event_ids(id)
    doc_ids = EventDocumentBuilder.build_document_ids(id, event_definition)
    {:ok, {event_ids, doc_ids}}
  end

  defp create_event(%{event_definition: event_definition}) do
    case EventsContext.create_event(%{event_definition_id: event_definition.id}) do
      {:ok, %{id: event_id}} ->
        {:ok, {event_id, nil, nil}}

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "create_event/1 failed with reason: #{inspect(reason)}"
        )

        raise "create_event/1 failed"
    end
  end

  defp delete_event(%{event_definition: event_definition} = data) do
    # Delete event -> get all data to remove + create a new event
    # append new event to the list of data to remove
    {:ok, {event_ids, doc_ids}} = fetch_data_to_delete(data)

    case EventsContext.create_event(%{event_definition_id: event_definition.id}) do
      {:ok, %{id: event_id}} ->
        {:ok, {event_id, event_ids ++ [event_id], doc_ids}}

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "delete_event/1 failed with reason: #{inspect(reason)}"
        )

        raise "delete_event/1 failed"
    end
  end

  defp update_event(%{event_definition: event_definition} = data) do
    # Update event -> get all data to remove + create a new event
    {:ok, {event_ids, doc_ids}} = fetch_data_to_delete(data)

    case EventsContext.create_event(%{event_definition_id: event_definition.id}) do
      {:ok, %{id: event_id}} ->
        {:ok, {event_id, event_ids, doc_ids}}

      {:error, reason} ->
        CogyntLogger.error(
          "Event Processor",
          "update_event/1 failed with reason: #{inspect(reason)}"
        )

        raise "update_event/1 failed"
    end
  end

  defp update_event_docs(event_docs, event_doc_ids) do
    case is_nil(event_doc_ids) or Enum.empty?(event_doc_ids) do
      true ->
        {:ok, _} = Elasticsearch.bulk_upsert_document(Config.event_index_alias(), event_docs)

      false ->
        {:ok, _} = Elasticsearch.bulk_delete_document(Config.event_index_alias(), event_doc_ids)
    end
  end

  defp update_risk_history_doc(risk_history_doc) do
    case !is_nil(risk_history_doc) do
      true ->
        {:ok, _} =
          Elasticsearch.upsert_document(
            Config.risk_history_index_alias(),
            risk_history_doc,
            risk_history_doc.id
          )

      false ->
        :ok
    end
  end
end
