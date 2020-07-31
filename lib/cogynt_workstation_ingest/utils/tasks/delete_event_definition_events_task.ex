defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteEventDefinitionEventsTask do
  @moduledoc """
  Task module that can be called to paginate through the events of an event_definition and updates the
  deleted_at using the new deleted_at value of the event_definition.
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

      EventsContext.delete_event_definition_data(event_definition)
      ConsumerStateManager.manage_request(%{stop_consumer: event_definition_id})

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

      EventsContext.update_event_definition(event_definition, %{started_at: nil})

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

    EventsContext.update_events(
      %{filter: %{event_ids: event_ids}},
      set: [deleted_at: deleted_at]
    )

    EventsContext.update_event_links(
      %{filter: %{linkage_event_ids: event_ids}},
      set: [deleted_at: deleted_at]
    )

    if page_number >= total_pages do
      ConsumerStateManager.remove_consumer_state(event_definition_id)
      Redis.publish_async("event_count_subscription", event_definition_id)

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

      Redis.publish_async("event_count_subscription", event_definition_id)

      process_page(next_page, event_definition)
    end

    {:ok, :success}
  end
end
