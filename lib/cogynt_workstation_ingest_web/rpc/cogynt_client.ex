defmodule CogyntWorkstationIngestWeb.Rpc.CogyntClient do
  alias JSONRPC2.Clients.HTTP

  @path "/rpc/cogynt"

  def publish_deleted_notifications(event_ids) when is_list(event_ids) do
    url = "#{service_name()}:#{service_port()}#{@path}"
    response = HTTP.call(url, "publish:deleted_notifications", event_ids)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end

  def publish_notifications(notifications) when is_list(notifications) do
    url = "#{service_name()}:#{service_port()}#{@path}"

    response = HTTP.call(url, "publish:subscriptions", notification_struct_to_map(notifications))

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end

  def publish_event_definition_ids(event_definition_ids) when is_list(event_definition_ids) do
    url = "#{service_name()}:#{service_port()}#{@path}"
    response = HTTP.call(url, "publish:event_definition_ids", event_definition_ids)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp notification_struct_to_map(notifications) do
    Enum.reduce(notifications, [], fn notification, acc ->
      acc ++
        [
          %{
            event_id: notification.event_id,
            user_id: notification.user_id,
            tag_id: notification.tag_id,
            id: notification.id,
            title: notification.title,
            notification_setting_id: notification.notification_setting_id,
            created_at: notification.created_at,
            updated_at: notification.updated_at
          }
        ]
    end)
  end

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp config(), do: Application.get_env(:cogynt_workstation_ingest, :rpc)
  defp service_name(), do: config()[:cogynt_otp_service_name]
  defp service_port(), do: config()[:cogynt_otp_service_port]
end
