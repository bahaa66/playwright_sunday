defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  import Ecto.Query
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Repo
  alias Models.Events.{Event, EventDetail, EventLink}
  alias Models.Notifications.{Notification}
  alias CogyntWorkstationIngest.Elasticsearch.EventDocument
  alias CogyntWorkstationIngestWeb.Rpc.IngestClient

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]

  @doc """
  Requires event fields in the data map. process_entities/1 will parse the entities keys value
  and pull out just the "id" fields. Ex: ${"locations" => [1, 2, 3], "accounts" => [5, 6]}. Will
  udpate the data map with a new :link_entities value storing the return value.
  """
  def process_entities(%{event: %{@entities => entities} = _event} = data) do
    link_entities =
      Enum.reduce(entities, %{}, fn {key, event_definition_list}, acc_map ->
        link_ids =
          Enum.reduce(event_definition_list, [], fn event_definition, acc_list ->
            acc_list ++ [event_definition["id"]]
          end)

        Map.put_new(acc_map, key, link_ids)
      end)

    Map.put(data, :link_entities, link_entities)
  end

  @doc """
  Requires link_entitites field in the data map. process_entity_ids/1 iterrates through the link_entities
  and checks to make sure that each id exists in the EventDetails table. If it does then it builds a list
  link_ids based on the event_id returned from the query. If it does not it raises an error that the
  Event is not ready for processing so that it is retried. Will update the data map with :link_ids
  field and store the return value.
  """
  def process_entity_ids(%{link_entities: link_entities} = data) do
    %{
      ready_to_process: ready_to_process,
      all_ids: link_ids
    } =
      Enum.reduce_while(link_entities, %{ready_to_process: true, all_ids: %{}}, fn {key, ids},
                                                                                   acc ->
        if acc.ready_to_process == true,
          do: {
            :cont,
            build_accumulator(key, ids, acc)
          },
          else: {:halt, acc}
      end)

    if ready_to_process == false do
      raise "LinkEvent is not ready for processing. Events linked do not exist"
    else
      Map.put(data, :link_ids, link_ids)
    end
  end

  @doc """
  Requires event_id and link_ids fields in the data map. process_event_links/1 creates the parent
  and child mappings for each id in link_ids. Will update the data map with :event_links field
  and with the return value.
  """
  def process_event_links(%{event_id: event_id, link_ids: link_ids} = data) do
    event_links =
      Enum.reduce(link_ids, [], fn {key_0, value_0}, acc_0 ->
        event_links =
          Enum.reduce(link_ids, [], fn {key_1, value_1}, acc_1 ->
            if value_0 != value_1 do
              acc_1 ++
                [%{parent_event_id: key_0, child_event_id: key_1, linkage_event_id: event_id}]
            else
              acc_1
            end
          end)

        acc_0 ++ event_links
      end)

    Map.put(data, :event_links, event_links)
  end

  def process_event_links(%{event_id: nil} = data), do: data

  @doc """
  Requires :event_details, :notifications, :elasticsearch_docs, :delete_ids, and :delete_docs
  fields in the data map. Takes all the fields and executes them in one databse transaction.
  """
  def execute_transaction(
        %{
          event_details: event_details,
          notifications: notifications,
          elasticsearch_docs: docs,
          event_links: event_links,
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

            # l_query =
            #   from(
            #     l in EventLinks,
            #     where: l.linkage_event_id in ^event_ids
            #   )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])
          #|> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
      end

    multi
    |> Multi.insert_all(:insert_event_detials, EventDetail, event_details)
    |> Multi.insert_all(:insert_notifications, Notification, notifications)
    |> Multi.insert_all(:insert_event_links, EventLink, event_links)
    |> Repo.transaction()

    case is_nil(doc_ids) or Enum.empty?(doc_ids) do
      true ->
        EventDocument.bulk_upsert_document(docs)

      false ->
        EventDocument.bulk_delete_document(doc_ids)
        EventDocument.bulk_upsert_document(docs)
    end

    # TODO: Need to format the correct prams to send to Cogynt-OTP
    # Send deleted_notifications to subscription_queue
    # IngestClient.publish_deleted_notifications(event_ids)
    # Send created_notifications to subscription_queue
    # IngestClient.publish_subscriptions(notifications)

    {:ok, data}
  end

  def execute_transaction(%{event_id: nil} = data), do: {:ok, data}

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp build_accumulator(key, values, %{all_ids: all_ids}) do
    %{ready_to_process: ready_to_process, ids: ids} =
      Enum.reduce_while(values, %{ready_to_process: true, ids: %{}}, fn id, acc ->
        if acc.ready_to_process == true,
          do: {
            :cont,
            build_accumulator(key, id, acc)
          },
          else: {:halt, acc}
      end)

    %{
      :ready_to_process => ready_to_process,
      :all_ids => Map.merge(all_ids, ids)
    }
  end

  defp build_accumulator(key, value, %{ids: ids}) do
    event_id =
      from(
        e in Event,
        join: ed in EventDetail,
        on: e.id == ed.event_id,
        where: ed.field_name == "id",
        where: ed.field_value == ^value,
        where: is_nil(e.deleted_at),
        limit: 1,
        select: e.id
      )
      |> Repo.one()

    case event_id != nil do
      true ->
        %{
          :ready_to_process => true,
          :ids => Map.put_new(ids, event_id, key)
        }

      false ->
        %{
          :ready_to_process => false,
          :ids => %{}
        }
    end
  end
end
