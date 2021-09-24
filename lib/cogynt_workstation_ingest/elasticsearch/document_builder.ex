defmodule CogyntWorkstationIngest.Elasticsearch.EventDocumentBuilder do
  @doc """
  Builds a document for the EventIndex
  """
  @spec build_document(
          event_id :: binary(),
          core_id :: binary(),
          title :: String.t(),
          event_definition_id :: binary(),
          event_details :: list(),
          published_at :: binary()
        ) :: {:ok, map()} | {:error, :invalid_data}
  def build_document(
        event_id,
        core_id,
        title,
        event_definition_id,
        event_details,
        published_at
      ) do
    with true <- is_binary(event_id),
         true <- is_binary(event_definition_id),
         true <- is_binary(title),
         true <- is_list(event_details) do
      case valid_core_id?(core_id) do
        true ->
          published_at =
            if is_nil(published_at) do
              DateTime.truncate(DateTime.utc_now(), :second)
            else
              published_at
            end

          result = %{
            id: event_id,
            title: title,
            core_event_id: core_id,
            event_definition_id: event_definition_id,
            event_details: event_details,
            published_at: published_at,
            created_at: DateTime.truncate(DateTime.utc_now(), :second),
            updated_at: DateTime.truncate(DateTime.utc_now(), :second)
          }

          {:ok, result}

        false ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Invalid core_id passed to document_builder/5. Core_id: #{core_id}"
          )

          {:error, :invalid_data}
      end
    else
      false ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Invalid core_id passed to document_builder/5. Possible invalid values. EventID: #{
            event_id
          }, EventDefinitionId: #{event_definition_id}, EventDetails: #{
            inspect(event_details, pretty: true)
          }"
        )

        {:error, :invalid_data}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #

  defp valid_core_id?(core_id) do
    case is_nil(core_id) do
      true ->
        true

      false ->
        is_binary(core_id)
    end
  end
end
