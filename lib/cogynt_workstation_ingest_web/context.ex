defmodule CogyntWorkstationIngestWeb.Context do
  @behaviour Plug
  import Plug.Conn
  alias CogyntWorkstationIngestWeb.Rpc.AuthClient
  alias CogyntGraphql.Utils.Error

  def init(opts), do: opts

  def call(conn, _) do
    case build_context(conn) do
      {:ok, context} ->
        Absinthe.Plug.put_options(conn, context: context)

      {:error, error} ->
        {:ok, res} =
          Jason.encode(%{
            errors: [error |> Error.normalize()]
          })

        conn
        |> send_resp(200, res)
        |> halt()
    end
  end

  def build_context(conn) do
    conn = fetch_session(conn)

    add_current_user(conn)
    |> add_session_state_id(conn)
    |> add_auth_provider(conn)
    |> add_client_ip(conn)
  end

  def add_current_user(conn) do
    case get_session(conn, :current_user) do
      nil ->
        {:ok, %{}}

      %{id: user_id} ->
        case AuthClient.fetch_user(user_id) do
          {:ok, user} ->
            {:ok, %{current_user: user}}

          {:error, :internal_server_error, original_error} ->
            error =
              Error.new(%{
                message: "Unable to connect to authentication server.",
                code: :service_unavailable,
                details:
                  "There was an error connecting to authentication server. This could be do to the authentication server being down or a timeout.",
                original_error: original_error,
                module: "#{__MODULE__} line: #{__ENV__.line}"
              })

            CogyntLogger.error(
              "#{__MODULE__}",
              inspect(error, pretty: true)
            )

            {:error, error}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__} line: #{__ENV__.line}",
              "Error returned while trying to authenticate user: #{inspect(error, pretty: true)}"
            )

            {:ok, %{current_user: nil}}
        end
    end
  end

  defp add_client_ip({:ok, context}, %{remote_ip: remote_ip}) when is_tuple(remote_ip) do
    client_ip = remote_ip |> Tuple.to_list() |> Enum.join(".")
    {:ok, Map.put(context, :client_ip, client_ip)}
  end

  defp add_client_ip(context, _conn), do: context

  defp add_session_state_id({:ok, context}, conn) do
    case get_session(conn, :session_state_id) do
      nil -> {:ok, context}
      id -> {:ok, Map.put(context, :session_state_id, id)}
    end
  end

  defp add_session_state_id(context, _conn), do: context

  defp add_auth_provider({:ok, context}, conn) do
    case get_session(conn, :auth_provider) do
      nil -> {:ok, context}
      provider -> {:ok, Map.put(context, :auth_provider, provider)}
    end
  end

  defp add_auth_provider(context, _conn), do: context
end
