defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the LinkEventPipeline.
  """
  alias CogyntWorkstationIngest.Config

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(%{event: event, crud_action: action, event_type: event_type} = data) do
    cond do
      event_type != Config.linkage_data_type_value() ->
        data
        |> Map.put(:validated, false)
        |> Map.put(:pipeline_state, :validate_link_event)

      action == Config.crud_delete_value() ->
        data
        |> Map.put(:pipeline_state, :validate_link_event)

      true ->
        case Map.get(event, Config.entities_key()) do
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
  end

  @doc """
  Itterates through the entities object on the link_event and builds the associations
  """
  def process_entities(%{validated: false} = data) do
    data
    |> Map.put(:elastic_event_links, nil)
    |> Map.put(:pipeline_state, :process_entities)
  end

  def process_entities(%{event: event, core_id: core_id, crud_action: crud_action} = data) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    cond do
      crud_action == Config.crud_delete_value() ->
        data
        |> Map.put(:elastic_event_links, nil)
        |> Map.put(:pipeline_state, :process_entities)

      true ->
        entities = Map.get(event, Config.entities_key())

        {pg_event_link_string, pg_event_link_list} =
          Enum.reduce(entities, {"", []}, fn {edge_label, link_data_list},
                                             {pg_string_acc, pg_list_acc} ->
            {pg_string, pg_list} =
              Enum.reduce(link_data_list, {"", []}, fn link_object,
                                                       {pg_string_acc_0, pg_list_acc_0} ->
                case link_object[Config.id_key()] do
                  nil ->
                    CogyntLogger.warn(
                      "#{__MODULE__}",
                      "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
                    )

                    {pg_string_acc_0, pg_list_acc_0}

                  entity_core_id ->
                    now = DateTime.truncate(DateTime.utc_now(), :second)

                    pg_string_acc_0 =
                      if pg_string_acc_0 != "" do
                        pg_string_acc_0 <>
                          "," <>
                          "'(#{core_id},#{entity_core_id},#{edge_label || "NULL"},#{now},#{now})'"
                      else
                        "'(#{core_id},#{entity_core_id},#{edge_label || "NULL"},#{now},#{now})'"
                      end

                    pg_list_acc_0 =
                      pg_list_acc_0 ++
                        [
                          %{
                            link_core_id: core_id,
                            entity_core_id: entity_core_id,
                            label: edge_label,
                            created_at: now,
                            updated_at: now
                          }
                        ]

                    {pg_string_acc_0, pg_list_acc_0}
                end
              end)

            pg_string_acc =
              if pg_string_acc != "" do
                pg_string_acc <> "," <> pg_string
              else
                pg_string
              end

            pg_list_acc = pg_list_acc ++ pg_list
            {pg_string_acc, pg_list_acc}
          end)

        pg_event_links_delete =
          cond do
            crud_action == Config.crud_update_value() ->
              [core_id]

            true ->
              []
          end

        data =
          Map.put(data, :pg_event_links, pg_event_link_string)
          |> Map.put(:pg_event_links_delete, pg_event_links_delete)
          |> Map.put(:elastic_event_links, pg_event_link_list)
          |> Map.put(:pipeline_state, :process_entities)

        # Execute telemtry for metrics
        :telemetry.execute(
          [:broadway, :process_entities],
          %{duration: System.monotonic_time() - start},
          telemetry_metadata
        )

        data
    end
  end
end
