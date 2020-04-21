defmodule CogyntWorkstationIngest.Utils.DeleteEventDefinitionEventsTask do
  @moduledoc """
  Task module that can be called to paginate through the events of an event_definition and updates the
  deleted_at using the new deleted_at value of the event_definition.
  """
  use Task
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition

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
        "Delete Event Definition Events Task",
        "Running delete event definition events task for ID: #{event_definition_id}"
      )

      page =
        EventsContext.paginate_events_by_event_definition_id(
          event_definition_id,
          1,
          @page_size,
          preload_details: false,
          include_deleted: true
        )

      process_page(page, event_definition)
    else
      nil ->
        CogyntLogger.warn(
          "Delete Event Definition Events Task",
          "Event definition not found for ID: #{event_definition_id}"
        )
    end
  end

  defp process_page(page, %{id: event_definition_id, deleted_at: deleted_at} = event_definition) do
    event_ids = Enum.map(page.entries, fn e -> e.id end)

    EventsContext.update_events(
      %{filter: %{event_ids: event_ids}},
      set: [deleted_at: deleted_at]
    )

    if page.page_number >= page.total_pages do
      CogyntLogger.info(
        "Delete Event Definition Events Task",
        "Finished processing events for event definition with the ID: #{event_definition_id}"
      )
    else
      new_page =
        EventsContext.paginate_events_by_event_definition_id(
          event_definition_id,
          page.page_number + 1,
          @page_size,
          preload_details: false,
          include_deleted: true
        )

      process_page(new_page, event_definition)
    end

    {:ok, :success}
  end
end
