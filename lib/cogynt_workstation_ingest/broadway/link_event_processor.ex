defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Events.EventsContext
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
  def execute_transaction(%{validated: false} = data), do: data
  def execute_transaction(%{event_id: nil} = data), do: data

  def execute_transaction(
        %{event_details: _event_details, delete_ids: _event_ids, event_links: _event_links} = data
      ) do
    case EventsContext.execute_link_event_processor_transaction(data) do
      {:ok, _result} ->
        data

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_transaction/1 failed with reason: #{inspect(reason)}"
        )

        raise "execute_transaction/1 failed"
    end
  end
end
