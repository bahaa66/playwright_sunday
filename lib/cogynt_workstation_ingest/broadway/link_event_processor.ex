defmodule CogyntWorkstationIngest.Broadway.LinkEventProcessor do
  alias CogyntWorkstationIngest.Config
  alias Broadway.Message

  @doc """
  Checks to make sure if a valid link event was passed through authoring. If incomplete data
  then :validated is set to false. Otherwise it is set to true.
  """
  def validate_link_event(
        %{
          crud_action: action,
          kafka_event: event,
          event_definition: %{event_type: event_type}
        } = data
      ) do
    cond do
      Atom.to_string(event_type) != Config.linkage_data_type_value() ->
        data
        |> Map.put(:validated, false)
        |> Map.put(:pipeline_state, :validate_link_event)

      action == Config.crud_delete_value() ->
        data
        |> Map.put(:pipeline_state, :validate_link_event)

      true ->
        case Map.get(event, String.to_atom(Config.entities_key())) do
          nil ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "link event missing entities field. LinkEvent: #{inspect(event, pretty: true)}"
            )

            data
            |> Map.put(:validated, false)
            |> Map.put(:pipeline_state, :validate_link_event)

          entities ->
            if Enum.empty?(entities) do
              CogyntLogger.warn(
                "#{__MODULE__}",
                "entity field is empty. Entity: #{inspect(entities, pretty: true)}"
              )

              data
              |> Map.put(:validated, false)
              |> Map.put(:pipeline_state, :validate_link_event)
            else
              data
              |> Map.put(:validated, true)
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

  def process_entities(%{core_id: core_id, crud_action: action, kafka_event: event} = data) do
    # Start timer for telemetry metrics
    start = System.monotonic_time()
    telemetry_metadata = %{}

    if action == Config.crud_delete_value() do
      data
      |> Map.put(:elastic_event_links, nil)
      |> Map.put(:pipeline_state, :process_entities)
    else
      entities = Map.get(event, String.to_atom(Config.entities_key()))

      {pg_event_link_list, pg_event_link_map} =
        Enum.reduce(entities, {[], []}, fn
          {edge_label, link_data_list}, {pg_list_acc, pg_map_acc} ->
            {pg_list, pg_map} =
              Enum.reduce(link_data_list, {[], []}, fn
                link_object, {pg_list_acc_0, pg_map_acc_0} ->
                  case link_object[String.to_atom(Config.id_key())] do
                    nil ->
                      CogyntLogger.warn(
                        "#{__MODULE__}",
                        "link object missing id field. LinkObject: #{inspect(link_object, pretty: true)}"
                      )

                      {pg_list_acc_0, pg_map_acc_0}

                    entity_core_id ->
                      now = DateTime.truncate(DateTime.utc_now(), :second)

                      pg_list_acc_0 =
                        pg_list_acc_0 ++
                          [
                            "#{core_id}\t#{entity_core_id}\t#{edge_label}\t#{now}\t#{now}\n"
                          ]

                      pg_map_acc_0 =
                        pg_map_acc_0 ++
                          [
                            %{
                              link_core_id: core_id,
                              entity_core_id: entity_core_id,
                              label: Atom.to_string(edge_label),
                              created_at: now,
                              updated_at: now
                            }
                          ]

                      {pg_list_acc_0, pg_map_acc_0}
                  end
              end)

            pg_list_acc = pg_list_acc ++ pg_list
            pg_map_acc = pg_map_acc ++ pg_map
            {pg_list_acc, pg_map_acc}
        end)

      pg_event_links_delete = if action == Config.crud_update_value(), do: [core_id], else: []

      data =
        Map.put(data, :pg_event_links, pg_event_link_list)
        |> Map.put(:pg_event_links_delete, pg_event_links_delete)
        |> Map.put(:elastic_event_links, pg_event_link_map)
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
