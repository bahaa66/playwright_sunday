defimpl Elasticsearch.Document, for: Models.Events.Event do
  def id(event), do: event.core_id
  def routing(_), do: false
  def encode(event) do
    %{
      core_id: event.core_id,
      event_definition_id: event.event_definition_id,
      occurred_at: event.occurred_at,
      risk_score: event.risk_score,
      event_details: event.event_details,
      created_at: event.created_at,
      updated_at: event.updated_at
    }
  end
end
