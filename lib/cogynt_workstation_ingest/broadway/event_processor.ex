defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  import Ecto.Query
  alias Ecto.Multi

  alias Models.Events.{Event, EventDetail}
  alias Models.Notifications.{Notification, NotificationSetting}
  alias CogyntWorkstationIngest.Elasticsearch.{EventDocument, RiskHistoryDocument}
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient

  @crud Application.get_env(:cogynt_workstation_ingest, :core_keys)[:crud]
  @risk_score Application.get_env(:cogynt_workstation_ingest, :core_keys)[:risk_score]
  @partial Application.get_env(:cogynt_workstation_ingest, :core_keys)[:partial]
  @update Application.get_env(:cogynt_workstation_ingest, :core_keys)[:update]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]
  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]

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
    {:ok, %{id: event_id}} =
      %Event{}
      |> Event.changeset(%{event_definition_id: event_definition.id})
      |> Repo.insert()

    Map.put(data, :event_id, event_id)
    |> Map.put(:delete_ids, nil)
    |> Map.put(:delete_docs, nil)
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
    event = Map.drop(event, [@crud, @partial])

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
            updated_docs =
              if(field_name != @entities) do
                acc_docs ++
                  [
                    EventDocument.build_document(
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
    |> Map.put(:risk_history_doc, RiskHistoryDocument.build_document(event_id, event))
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
        ns_query =
          from(ns in NotificationSetting,
            where: ns.event_definition_id == type(^event_definition.id, :binary_id),
            where: ns.active == true
          )

        {:ok, notifications} =
          Repo.transaction(fn ->
            Repo.stream(ns_query)
            |> Stream.map(fn ns ->
              case Map.has_key?(event_definition.fields, ns.title) do
                true ->
                  %{
                    event_id: event_id,
                    user_id: ns.user_id,
                    # topic: event_definition.topic, TODO: do we need to pass this value ??
                    tag_id: ns.tag_id,
                    title: ns.title,
                    notification_setting_id: ns.id,
                    created_at: DateTime.truncate(DateTime.utc_now(), :second),
                    updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                    # TODO: do we need to pass this value ??
                    # Optional attribute, MUST use Map.get
                    # description: Map.get(ns, :description)
                  }

                false ->
                  %{}
              end
            end)
            |> Enum.to_list()
          end)

        if Enum.empty?(notifications) do
          Map.put(data, :notifications, nil)
        else
          Map.put(data, :notifications, notifications)
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
          event_details: event_details,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          notifications: nil,
          delete_ids: event_ids,
          delete_docs: doc_ids
        } = data
      ) do
    multi =
      case is_nil(event_ids) or Enum.empty?(event_ids) do
        true ->
          Multi.new()

        false ->
          n_query =
            from(n in Notification,
              where: n.event_id in ^event_ids
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          multi =
            Multi.new()
            |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
            |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])

          # Send deleted_notifications to subscription_queue
          # TODO: improvement can possibly made to run a select during the transaction
          # and call the cogynt-client with the returned notifications
          CogyntClient.publish_deleted_notifications(event_ids)

          multi
      end

    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
    |> Repo.transaction()

    case is_nil(doc_ids) or Enum.empty?(doc_ids) do
      true ->
        EventDocument.bulk_upsert_document(event_docs)

      false ->
        EventDocument.bulk_delete_document(doc_ids)
        EventDocument.bulk_upsert_document(event_docs)
    end

    if !is_nil(risk_history_doc) do
      RiskHistoryDocument.upsert_document(risk_history_doc, risk_history_doc.id)
    end

    Map.put(data, :event_processed, true)
  end

  def execute_transaction(
        %{
          event_details: event_details,
          notifications: notifications,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_ids: event_ids,
          delete_docs: doc_ids
        } = data
      ) do
    multi =
      case is_nil(event_ids) or Enum.empty?(event_ids) do
        true ->
          Multi.new()

        false ->
          n_query =
            from(n in Notification,
              where: n.event_id in ^event_ids
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          multi =
            Multi.new()
            |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
            |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])

          # Send deleted_notifications to subscription_queue
          # TODO: improvement can possibly made to run a select during the transaction
          # and call the cogynt-client with the returned notifications
          CogyntClient.publish_deleted_notifications(event_ids)

          multi
      end

    {:ok, %{insert_notifications: {_count, created_notifications}}} =
      multi
      |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
      |> Multi.insert_all(:insert_notifications, Notification, notifications,
        returning: [
          :event_id,
          :user_id,
          :tag_id,
          :id,
          :title,
          :notification_setting_id,
          :created_at,
          :updated_at
        ]
      )
      |> Repo.transaction()

    case is_nil(doc_ids) or Enum.empty?(doc_ids) do
      true ->
        EventDocument.bulk_upsert_document(event_docs)

      false ->
        EventDocument.bulk_delete_document(doc_ids)
        EventDocument.bulk_upsert_document(event_docs)
    end

    if !is_nil(risk_history_doc) do
      RiskHistoryDocument.upsert_document(risk_history_doc, risk_history_doc.id)
    end

    # Send created_notifications to subscription_queue
    CogyntClient.publish_notifications(created_notifications)

    Map.put(data, :event_processed, true)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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

    if partial == nil or partial == false or risk_score > 0 do
      true
    else
      false
    end
  end

  defp fetch_data_to_delete(%{
         event: %{"id" => id},
         event_definition: event_definition
       }) do
    query =
      from(d in EventDetail,
        join: e in Event,
        on: e.id == d.event_id,
        where: d.field_value == ^id and is_nil(e.deleted_at),
        select: d.event_id
      )

    {:ok, event_ids} =
      Repo.transaction(fn ->
        Repo.stream(query)
        |> Enum.to_list()
      end)

    doc_ids = EventDocument.build_document_ids(id, event_definition)
    {:ok, {event_ids, doc_ids}}
  end

  defp create_event(%{event_definition: event_definition}) do
    {:ok, %{id: event_id}} =
      %Event{}
      |> Event.changeset(%{event_definition_id: event_definition.id})
      |> Repo.insert()

    {:ok, {event_id, nil, nil}}
  end

  defp delete_event(%{event_definition: event_definition} = data) do
    # Delete event -> get all data to remove + create a new event
    # append new event to the list of data to remove
    {:ok, {event_ids, doc_ids}} = fetch_data_to_delete(data)

    {:ok, %{id: event_id}} =
      %Event{}
      |> Event.changeset(%{event_definition_id: event_definition.id})
      |> Repo.insert()

    {:ok, {event_id, event_ids ++ [event_id], doc_ids}}
  end

  defp update_event(%{event_definition: event_definition} = data) do
    # Update event -> get all data to remove + create a new event
    {:ok, {event_ids, doc_ids}} = fetch_data_to_delete(data)

    {:ok, %{id: event_id}} =
      %Event{}
      |> Event.changeset(%{event_definition_id: event_definition.id})
      |> Repo.insert()

    {:ok, {event_id, event_ids, doc_ids}}
  end
end
