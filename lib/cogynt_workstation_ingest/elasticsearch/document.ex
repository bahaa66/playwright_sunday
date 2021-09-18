defimpl Elasticsearch.Document, for: Models.Events.Event do
  # alias CogyntWorkstationIngest.Events.EventsContext

  def id(event), do: event.core_id
  def routing(_), do: false

  def encode(event) do
    event_definition = event.event_definition
    event_definition_details = event_definition.event_definition_details
    # Iterate over each event key value pair and build the pg and elastic search event
    # details.
    elasticsearch_event_details =
      Enum.reduce(event.event_details, [], fn
        {key, value}, acc ->
          # Search the event definition details and use the path to figure out the field value.
          Enum.find_value(event_definition_details, fn
            %{path: path, field_name: field_name, field_type: field_type} ->
              # Split the path on the delimiter which currently is hard coded to |
              case String.split(path, "|") do
                # If there is only one element in the list then we don't need to dig into the object
                # any further and we return the value.
                [first] when first == key ->
                  value

                [first | remaining_path] when first == key ->
                  # If the path is has a length is greater than 1 then whe use it to get the value.
                  Enum.reduce(remaining_path, value, fn
                    p, a when is_map(a) ->
                      Map.get(a, p)

                    _, _ ->
                      nil
                  end)
                  |> case do
                    nil ->
                      CogyntLogger.warn(
                        "#{__MODULE__}",
                        "Could not find value at given Path: #{inspect(path)}"
                      )

                      false

                    value ->
                      value
                  end

                _ ->
                  nil
              end
              # Convert the value if needed
              |> case do
                nil -> false
                value when is_binary(value) -> {value, field_name, field_type, path}
                value -> {Jason.encode!(value), field_name, field_type, path}
              end
          end)
          |> case do
            # If it has a field type then it has a corresponding event definition detail that gives
            # us the the field_type so we save an event_detail and a elastic document
            {field_value, field_name, field_type, path} ->
              acc ++
                [
                  %{
                    field_name: field_name,
                    field_type: field_type,
                    field_value: field_value,
                    path: path
                  }
                ]

            nil ->
              acc
          end
      end)

    %{
      id: event.core_id,
      event_definition_id: event.event_definition_id,
      core_event_id: event.core_id,
      occurred_at: event.occurred_at,
      converted_risk_score: event.risk_score,
      created_at: event.created_at,
      updated_at: event.updated_at,
      title: event_definition.title,
      event_type: event_definition.event_type,
      event_details: elasticsearch_event_details,
    }
    |> IO.inspect()
end


end
