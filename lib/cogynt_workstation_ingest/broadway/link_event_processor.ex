defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Broadway.Message

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
    entity_links_core_ids =
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
                acc_1 ++ [core_id]
            end
          end)

        acc_0 ++ objects_links
      end)

    deleted_at =
      if action == @delete do
        DateTime.truncate(DateTime.utc_now(), :second)
      else
        nil
      end

    case EventsContext.call_insert_event_links_function(
           event_id,
           entity_links_core_ids,
           deleted_at
         ) do
      {:ok, _result} ->
        data = Map.put(data, :pipeline_state, :process_entities)
        Map.put(message, :data, data)

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to insert link_events. Error: #{inspect(error)}"
        )

        raise "process_entities/1 failed"
    end
  end
end
