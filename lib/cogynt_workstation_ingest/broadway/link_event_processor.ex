defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Config

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%{event: event} = data) do
    case Map.get(event, @entities) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "link event missing entities field. LinkEvent: #{inspect(event)}"
        )

        Map.put(data, :validated, false)

      entities ->
        if Enum.empty?(entities) or Enum.count(entities) == 1 do
          CogyntLogger.warn(
            "#{__MODULE__}",
            "entity field is empty or only has 1 link obect. Entity: #{inspect(entities)}"
          )

          Map.put(data, :validated, false)
        else
          Map.put(data, :validated, true)
        end
    end
  end

  @doc """
  Requires event fields in the data map. process_entities/1 will parse the entities keys value
  and pull out just the "id" fields. Ex: ${"locations" => [1, 2, 3], "accounts" => [5, 6]}. Will
  udpate the data map with a new :link_entities value storing the return value.
  """
  def process_entities(%{validated: false} = data), do: data
  def process_entities(%{event_id: nil} = data), do: data

  def process_entities(%{event: %{@entities => entities}, event_id: event_id} = data) do
    entity_links =
      Enum.reduce(entities, [], fn {_key, link_object_list}, acc_0 ->
        objects_links =
          Enum.reduce(link_object_list, [], fn link_object, acc_1 ->
            case link_object["id"] do
              nil ->
                CogyntLogger.warn(
                  "#{__MODULE__}",
                  "link object missing id field. LinkObject: #{inspect(link_object)}"
                )

                acc_1

              core_id ->
                acc_1 ++ [%{linkage_event_id: event_id, core_id: core_id}]
            end
          end)

        acc_0 ++ objects_links
      end)

    Map.put(data, :link_events, entity_links)
  end

  @doc """
  Requires :event_links fields in the data map. Takes all the fields and
  executes them in one databse transaction.
  """
  def execute_transaction(%{event_id: nil} = data), do: data

  def execute_transaction(
        %{
          notifications: _notifications,
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_docs: doc_ids
        } = data
      ) do
    case EventsContext.execute_pipeline_transaction(data) do
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
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason)}"
        )

        raise "execute_transaction/1 failed"
    end

    # elastic search updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    data
  end

  def execute_transaction(
        %{
          event_docs: event_docs,
          risk_history_doc: risk_history_doc,
          delete_docs: doc_ids
        } = data
      ) do
    case EventsContext.execute_pipeline_transaction(data) do
      {:ok, %{update_notifications: {_count, deleted_notifications}}} ->
        CogyntClient.publish_deleted_notifications(deleted_notifications)

      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason)}"
        )

        raise "execute_transaction/1 failed"
    end

    # elastic search updates
    update_event_docs(event_docs, doc_ids)
    update_risk_history_doc(risk_history_doc)

    data
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
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
            risk_history_doc.id,
            risk_history_doc
          )

      false ->
        :ok
    end
  end
end
