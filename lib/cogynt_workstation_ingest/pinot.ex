defmodule CogyntWorkstationIngest.Pinot do
  @callback query(query :: %{required(:sql) => map}) :: {:ok, map()}
  @optional_callbacks query: 1

  defmacro __using__(_) do
    quote do
      @behaviour CogyntWorkstationIngest.Pinot

      @type api_error :: {:error, {integer(), map()}} | {:error, any()}

      @spec handle_response(response :: Tesla.Env.result()) ::
              {:ok, any()} | {:error, {integer(), map()}} | {:error, any()}
      defp handle_response(response) do
        response
        |> case do
          {:ok, %Tesla.Env{body: %{error: error, code: status}}} ->
            {:error, {status, error}}

          {:ok, %Tesla.Env{body: body}} ->
            {:ok, body}

          {:error, error} ->
            {:error, error}
        end
      end

      defoverridable handle_response: 1
    end
  end
end
