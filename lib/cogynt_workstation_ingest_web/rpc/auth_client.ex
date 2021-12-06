defmodule CogyntWorkstationIngestWeb.Rpc.AuthClient do
  alias CogyntWorkstationIngest.Config

  @path "/rpc/auth"

  def fetch_user(user_id) do
    "#{Config.auth_service_name()}:#{Config.auth_service_port()}#{@path}"
    |> Config.rpc_client().call("users:fetch_user", %{"id" => user_id})
    |> handle_response("fetch_user/1")
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp keys_to_atoms(list_response) when is_list(list_response) do
    Enum.reduce(list_response, [], fn i, acc ->
      [keys_to_atoms(i) | acc]
    end)
  end

  defp keys_to_atoms(string_key_map) when is_map(string_key_map) do
    Enum.reduce(string_key_map, %{}, fn
      {key, val}, a when is_map(val) or is_list(val) ->
        key =
          if is_binary(key) do
            String.to_atom(key)
          else
            key
          end

        Map.put(a, key, keys_to_atoms(val))

      {key, val}, a ->
        key =
          if is_binary(key) do
            String.to_atom(key)
          else
            key
          end

        Map.put(a, key, val)
    end)
  end

  defp keys_to_atoms(other), do: other

  defp handle_response({:ok, %{"body" => body, "status" => "ok"}}, _) do
    {:ok, keys_to_atoms(body)}
  end

  defp handle_response({:ok, %{"body" => body, "status" => "error"}}, caller) do
    CogyntLogger.error(
      "#{__MODULE__}",
      "#{caller} Error returned by Auth:\n#{inspect(body, pretty: true)}"
    )

    {:error, body}
  end

  defp handle_response({:error, error}, caller) do
    CogyntLogger.error(
      "#{__MODULE__}",
      "#{caller} Internal Server Error Conmunicating with Auth:\n#{inspect(error, pretty: true)}"
    )

    {:error, :internal_server_error, error}
  end
end
