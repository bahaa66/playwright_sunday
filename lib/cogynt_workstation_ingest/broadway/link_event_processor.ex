defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Config
  alias Broadway.Message

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]
  @defaults %{
    deleted_event_ids: nil,
    crud_action: nil,
    risk_history_document: nil,
    event_document: nil,
    notifications: nil
  }

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "validate_link_event/1 failed. No message data")
    message
  end

  def validate_link_event(%Message{data: %{event: event} = data} = message) do
    case Map.get(event, @entities) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "link event missing entities field. LinkEvent: #{inspect(event, pretty: true)}"
        )

        data = Map.put(data, :validated, false)
        Map.put(message, :data, data)

      entities ->
        data =
          if Enum.empty?(entities) do
            CogyntLogger.warn(
              "#{__MODULE__}",
              "entity field is empty. Entity: #{inspect(entities, pretty: true)}"
            )

            Map.put(data, :validated, false)
            |> Map.put(:pipeline_state, :validate_link_event)
          else
            Map.put(data, :validated, true)
            |> Map.put(:pipeline_state, :validate_link_event)
          end

        Map.put(message, :data, data)
    end
  end

  @doc """
  Requires event fields in the data map. process_entities/1 will parse the entities keys value
  and pull out just the "id" fields. Ex: ${"locations" => [1, 2, 3], "accounts" => [5, 6]}. Will
  udpate the data map with a new :link_entities value storing the return value.
  """
  def process_entities(%Message{data: nil} = message) do
    CogyntLogger.warn("#{__MODULE__}", "process_entities/1 failed. No message data")
    message
  end

  def process_entities(%Message{data: %{validated: false}} = message), do: message
  def process_entities(%Message{data: %{event_id: nil}} = message), do: message

  def process_entities(
        %Message{data: %{event: %{@entities => entities}, event_id: event_id} = data} = message
      ) do
    entity_links =
      Enum.reduce(entities, [], fn {_key, link_object_list}, acc_0 ->
        objects_links =
          Enum.reduce(link_object_list, [], fn link_object, acc_1 ->
            case link_object["id"] do
              nil ->
                CogyntLogger.warn(
                  "#{__MODULE__}",
                  "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
                )

                acc_1

              core_id ->
                acc_1 ++ [%{linkage_event_id: event_id, core_id: core_id}]
            end
          end)

        acc_0 ++ objects_links
      end)

    data =
      Map.put(data, :link_events, entity_links)
      |> Map.put(:pipeline_state, :process_entities)

    Map.put(message, :data, data)
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
      link_events: [],
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
                :retry_count,
                :validated
              ])

            Map.merge(acc_1, data, fn k, v1, v2 ->
              case k do
                :event_details ->
                  v1 ++ v2

                :link_events ->
                  v1 ++ v2

                :deleted_event_ids ->
                  if v2 == @defaults.deleted_event_ids or Enum.empty?(v2) do
                    v1
                  else
                    Enum.uniq(v1 ++ v2)
                  end

                :event_id ->
                  v2

                :crud_action ->
                  v2

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
      event_details: [],
      link_events: []
    }

    bulk_transactional_data =
      Enum.reduce(core_id_data_map, default_map, fn {_key, data}, acc ->
        data = Map.drop(data, [:event_id])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :link_events ->
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

    # Elasticsearch Updates
    # TODO: instead of creating all the documents and then in the next
    # step removing a subset of the documents you just created. Add a step
    # to just filter out those documents from the event_doc list so they are
    # never created
    if !Enum.empty?(bulk_transactional_data.event_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.event_index_alias(),
          bulk_transactional_data.event_doc
        )
    end

    if !Enum.empty?(bulk_transactional_data.deleted_event_ids) do
      {:ok, _result} =
        Elasticsearch.bulk_delete_document(
          Config.event_index_alias(),
          bulk_transactional_data.deleted_event_ids
        )
    end

    if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.risk_history_index_alias(),
          bulk_transactional_data.risk_history_doc
        )
    end

    # Build database transaction
    transaction_result =
      EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
      |> EventsContext.insert_all_event_links_multi(bulk_transactional_data.link_events)
      |> EventsContext.update_all_event_links_multi(bulk_transactional_data.delete_event_ids)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_batch_transaction_for_crud/1 failed with reason: #{
            inspect(reason, pretty: true)
          }"
        )

        raise "execute_batch_transaction_for_crud/1 failed"

      _ ->
        nil
    end
  end

  @doc """
  For datasets that can be processed in bulk this will aggregate all of the
  processed data and insert it into postgresql in bulk
  """
  def execute_batch_transaction(messages) do
    # build transactional data
    default_map = %{
      notifications: [],
      event_details: [],
      event_doc: [],
      risk_history_doc: [],
      link_events: []
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
            :retry_count,
            :validated
          ])

        Map.merge(acc, data, fn k, v1, v2 ->
          case k do
            :event_details ->
              v1 ++ v2

            :link_events ->
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

    # elasticsearch updates
    if !Enum.empty?(bulk_transactional_data.event_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.event_index_alias(),
          bulk_transactional_data.event_doc
        )
    end

    if !Enum.empty?(bulk_transactional_data.risk_history_doc) do
      {:ok, _} =
        Elasticsearch.bulk_upsert_document(
          Config.risk_history_index_alias(),
          bulk_transactional_data.risk_history_doc
        )
    end

    # Run database transaction
    transaction_result =
      EventsContext.insert_all_event_details_multi(bulk_transactional_data.event_details)
      |> EventsContext.insert_all_event_links_multi(bulk_transactional_data.link_events)
      |> EventsContext.run_multi_transaction()

    case transaction_result do
      {:ok, _} ->
        nil

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_batch_transaction/1 failed with reason: #{inspect(reason, pretty: true)}"
        )

        raise "execute_batch_transaction/1 failed"
    end

    messages
  end
end
