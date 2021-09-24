defmodule CogyntWorkstationIngest.Elasticsearch.EventDocumentBuilder do
  @moduledoc """
  Formats and Builds a document for the Event Index
  """

  @document_schema %{
    id: [type: :string, required: true],
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
    published_at: [type: :datetime, required: true],
    created_at: [type: :datetime, required: true],
    updated_at: [type: :datetime, required: true],
    occurred_at: [type: :datetime],
    risk_score: [type: :float],
    converted_risk_score: [type: :integer],
    lexicons: [type: {:list, :string}]
  }

  @doc """
  Builds a document for the EventIndex
  """
  @spec build_document(
          core_id :: binary(),
          title :: String.t(),
          event_definition_id :: binary(),
          event_details :: list(),
          published_at :: binary(),
          event_type :: atom()
        ) :: {:ok, map()} | {:error, :invalid_data}
  def build_document(
        core_id,
        title,
        event_definition_id,
        event_details,
        published_at,
        event_type \\ :none
      ) do
    published_at =
      if is_nil(published_at) do
        DateTime.truncate(DateTime.utc_now(), :second)
      else
        published_at
      end

    result = %{
      id: core_id,
      title: title,
      core_event_id: core_id,
      event_definition_id: event_definition_id,
      event_details: event_details,
      published_at: published_at,
      created_at: DateTime.truncate(DateTime.utc_now(), :second),
      updated_at: DateTime.truncate(DateTime.utc_now(), :second),
      event_type: event_type
    }

    validate_document(result)
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

  def build_document(parameters) when is_map(parameters) do
    now = DateTime.truncate(DateTime.utc_now(), :second)

    parameters =
      Map.put(parameters, :published_at, Map.get(parameters, :published_at) || now)
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
                          "#{Atom.to_string(p)} failed parameter validation with the error: #{
                            error
                          }"
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
end
