defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Broadway.Message
  alias Models.Enums.DeletedByValue

  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]
  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%Message{data: %{event: event} = data} = message) do
    case Map.get(event, @entities) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "link event missing entities field. LinkEvent: #{inspect(event, pretty: true)}"
        )

        data =
          Map.put(data, :validated, false)
          |> Map.put(:pipeline_state, :validate_link_event)

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
  def process_entities(%Message{data: %{validated: false} = data} = message) do
    data = Map.put(data, :pipeline_state, :process_entities)
    Map.put(message, :data, data)
  end

  def process_entities(%Message{data: %{event_id: nil} = data} = message) do
    data = Map.put(data, :pipeline_state, :process_entities)
    Map.put(message, :data, data)
  end

  def process_entities(
        %Message{
          data: %{event: %{@entities => entities}, event_id: event_id, crud_action: action} = data
        } = message
      ) do
    {deleted_at, deleted_by} =
      if action == @delete do
        {DateTime.truncate(DateTime.utc_now(), :second), DeletedByValue.Crud}
      else
        {nil, nil}
      end

    Enum.reduce(entities, [], fn {edge_label, link_data_list}, acc ->
      link_object = List.first(link_data_list) || %{}

      case link_object["id"] do
        nil ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
          )

          acc

        core_id ->
          acc ++
            [
              %{
                linkage_event_id: event_id,
                label: edge_label,
                core_id: core_id,
                deleted_at: deleted_at,
                deleted_by: deleted_by
              }
            ]
      end
    end)
    |> EventsContext.insert_all_event_links()

    data = Map.put(data, :pipeline_state, :process_entities)
    Map.put(message, :data, data)
  end
end
