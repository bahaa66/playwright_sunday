defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  @entities Application.get_env(:cogynt_workstation_ingest, :core_keys)[:entities]

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%{crud_action: "delete"} = data), do: data

  def validate_link_event(%{event: event} = data) do
    case Map.get(event, @entities) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "link event missing entities field. LinkEvent: #{inspect(event, pretty: true)}"
        )

        Map.put(data, :validated, false)
        |> Map.put(:pipeline_state, :validate_link_event)

      entities ->
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
    end
  end

  @doc """
  """
  def process_entities(%{crud_action: "delete"} = data), do: data

  def process_entities(%{validated: false} = data),
    do: Map.put(data, :pipeline_state, :process_entities)

  def process_entities(
        %{event: %{@entities => entities}, core_id: core_id, crud_action: crud_action} = data
      ) do
    pg_event_links =
      Enum.reduce(entities, [], fn {edge_label, link_data_list}, acc ->
        link_object = List.first(link_data_list) || %{}

        case link_object["id"] do
          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
            )

            acc

          entity_core_id ->
            now = DateTime.truncate(DateTime.utc_now(), :second)

            acc ++
              [
                %{
                  link_core_id: core_id,
                  label: edge_label,
                  entity_core_id: entity_core_id,
                  created_at: now,
                  updated_at: now
                }
              ]
        end
      end)

    pg_event_links_delete =
      case crud_action do
        "update" ->
          [core_id]

        _ ->
          []
      end

    Map.put(data, :pg_event_links, pg_event_links)
    |> Map.put(:pg_event_links_delete, pg_event_links_delete)
    |> Map.put(:pipeline_state, :process_entities)
  end
end
