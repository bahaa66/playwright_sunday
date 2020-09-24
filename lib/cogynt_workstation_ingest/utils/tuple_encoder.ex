defmodule CogyntWorkstationIngest.Utils.TupleEncoder do
  alias Jason.Encoder

  defimpl Encoder, for: Tuple do
    def encode(data, options) when is_tuple(data) do
      data
      |> build_list()
      |> Encoder.List.encode(options)
    end

    defp build_list(data) when is_tuple(data) do
      data
      |> Tuple.to_list()
      |> Enum.reduce([], fn item, acc ->
        acc ++ [build_list(item)]
      end)
    end

    defp build_list(data) do
      data
    end
  end
end
