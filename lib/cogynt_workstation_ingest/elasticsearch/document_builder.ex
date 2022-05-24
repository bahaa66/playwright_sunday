defmodule CogyntWorkstationIngest.Elasticsearch.EventDocumentBuilder do
  @moduledoc """
  Formats and Builds a document for the Event Index
  """

  @document_schema %{
    id: [type: :string, required: true],
    event_definition_hash_id: [type: :string, required: true],
    event_definition_id: [type: :string, required: true],
    title: [type: :string, required: true],
    event_details: [
      type:
        {:list,
         %{
           field_name: [type: :string, required: true],
           field_value: [type: :string, required: true],
           field_type: [type: :string, required: true],
           path: [type: :string, required: true]
         }},
      required: true
    ],
    core_event_id: [type: :string],
    event_type: [type: {:one_of, ["none", "entity", "linkage"]}, required: true],
    created_at: [type: :datetime, required: true],
    updated_at: [type: :datetime, required: true],
    occurred_at: [type: :datetime],
    risk_score: [type: :integer],
    event_links: [type: :object]
  }

  def build_document(parameters) do
    now = DateTime.truncate(DateTime.utc_now(), :second)

    parameters =
      parameters
      |> Map.put_new(:updated_at, now)
      |> Map.put(:created_at, now)

    validate_document(parameters)
    |> case do
      {:error, message} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          message
        )

        {:error, :invalid_data}

      {:ok, result} ->
        {:ok, result}
    end
  end

  def validate_document(parameters) do
    try do
      {:ok,
       Enum.reduce(@document_schema, %{}, fn
         {p, restrictions}, a ->
           value = Map.get(parameters, p)

           try do
             Map.put(a, p, validate_parameter!(value, restrictions))
           catch
             error ->
               throw("#{Atom.to_string(p)} failed parameter validation with the error: #{error}")
           end
       end)}
    catch
      error -> {:error, error}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp validate_parameter!(nil, restrictions),
    do: if(Keyword.get(restrictions, :required, false), do: throw("is required"))

  defp validate_parameter!(value, restrictions) do
    Enum.each(restrictions, &validate_restriction!(value, &1))
    value
  end

  defp validate_restriction!(value, {:required, required}) do
    if required and is_nil(value) do
      throw("is required")
    end

    value
  end

  defp validate_restriction!(value, {:type, :string}) when is_binary(value), do: value

  defp validate_restriction!(value, {:type, :string}),
    do: throw("#{inspect(value)} must be a string")

  defp validate_restriction!(value, {:type, :datetime}) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _} -> datetime
      {:error, :invalid_format} -> throw("#{value} could not be converted to a datetime")
    end
  end

  defp validate_restriction!(%DateTime{} = value, {:type, :datetime}), do: value

  defp validate_restriction!(value, {:type, :datetime}),
    do: throw("#{inspect(value)} must be a valid datetime")

  defp validate_restriction!(values, {:type, {:list, type}}) when is_list(values) do
    cond do
      is_map(type) ->
        Enum.reduce(type, [], fn
          {p, restrictions}, a ->
            a ++
              [
                Enum.reduce(values, %{}, fn
                  i, a ->
                    value = Map.get(i, p)

                    try do
                      Map.put(a, p, validate_parameter!(value, restrictions))
                    catch
                      error ->
                        throw(
                          "#{Atom.to_string(p)} failed parameter validation with the error: #{error}"
                        )
                    end
                end)
              ]
        end)

      true ->
        Enum.map(values, &validate_restriction!(&1, {:type, type}))
    end
  end

  defp validate_restriction!(value, {:type, {:list, _type}}),
    do: throw("#{inspect(value)} must be a list")

  defp validate_restriction!(value, {:type, {:one_of, one_of}}),
    do:
      if(value in one_of,
        do: value,
        else: throw("#{inspect(value)} must be one of #{inspect(one_of)}")
      )

  defp validate_restriction!(value, {:type, :float}) when is_float(value), do: value

  defp validate_restriction!(value, {:type, :float}) when is_integer(value), do: value / 1

  defp validate_restriction!(value, {:type, :float}) when is_binary(value) do
    Float.parse(value)
    |> case do
      {value, _} -> value
      :error -> throw("could not parse #{inspect(value)} to a float")
    end
  end

  defp validate_restriction!(value, {:type, :float}),
    do: throw("#{inspect(value)} must be a valid float")

  defp validate_restriction!(value, {:type, :integer}) when is_integer(value), do: value

  defp validate_restriction!(value, {:type, :integer}),
    do: throw("#{inspect(value)} must be a valid integer")

  defp validate_restriction!(value, {:type, :object}) when is_map(value), do: value

  defp validate_restriction!(value, {:type, :object}),
    do: throw("#{inspect(value)} must be a valid map")
end
