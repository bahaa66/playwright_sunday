defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteEventDefinitionEventsTask do
  @moduledoc """
  Task module that can be called to paginate through the events of an event_definition and updates the
  deleted_at using the new deleted_at value of the event_definition. This is used to soft_delete the event_definition
  and all data associated with it.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager

  @page_size 2000

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(event_definition_id), do: update_event_definition_events(event_definition_id)

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp update_event_definition_events(event_definition_id) do
    with %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(event_definition_id) do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Running delete event definition events task for ID: #{event_definition_id}"
      )

      # First stop the consumer
      ConsumerStateManager.manage_request(%{stop_consumer: event_definition_id})

      # Second soft delete_event_definition_event_detail_templates_data˝
      EventsContext.delete_event_definition_event_detail_templates_data(event_definition)

      # Third remove all documents from elasticsearch
      Elasticsearch.delete_by_query(Config.event_index_alias(), %{
        field: "event_definition_id",
        value: event_definition_id
      })

      case EventsContext.get_core_ids_for_event_definition_id(event_definition_id) do
        [] ->
          nil

        core_ids ->
          Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
            field: "id",
            value: core_ids
          })
      end

      # Fourth paginate through all the events linked to the event_definition_id and
      # delete them
      page =
        EventsContext.get_page_of_events(
          %{filter: %{event_definition_id: event_definition.id}},
          page_number: 1,
          page_size: @page_size,
          preload_details: false,
          include_deleted: true
        )

      process_page(page, event_definition)
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Event definition not found for ID: #{event_definition_id}"
        )
    end
  end

  defp process_page(
         %{entries: entries, page_number: page_number, total_pages: total_pages},
         %{id: event_definition_id} = event_definition
       ) do
    deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

    event_ids = Enum.map(entries, fn e -> e.id end)

    # Update all events to be deleted
    EventsContext.update_events(
      %{filter: %{event_ids: event_ids}},
      set: [deleted_at: deleted_at]
    )

    # Update all event_links to be deleted
    EventsContext.update_event_links(
      %{filter: %{linkage_event_ids: event_ids}},
      set: [deleted_at: deleted_at]
    )

    if page_number >= total_pages do
      # Update event_definition to be inactive
      EventsContext.update_event_definition(event_definition, %{active: false, deleted_at: nil})
      # remove all state in Redis that is linked to event_definition_id
      ConsumerStateManager.remove_consumer_state(event_definition_id)
      Redis.publish_async("event_definitions_subscription", %{updated: event_definition_id})

      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished processing events for event definition with the ID: #{event_definition_id}"
      )
    else
      next_page =
        EventsContext.get_page_of_events(
          %{filter: %{event_definition_id: event_definition_id}},
          page_number: page_number + 1,
          page_size: @page_size,
          preload_details: false,
          include_deleted: true
        )

      Redis.publish_async("event_definitions_subscription", %{updated: event_definition_id})

      process_page(next_page, event_definition)
    end

    {:ok, :success}
  end
end
