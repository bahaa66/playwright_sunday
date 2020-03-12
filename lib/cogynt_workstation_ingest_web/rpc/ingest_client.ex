defmodule CogyntWorkstationIngestWeb.Rpc.IngestClient do
  alias JSONRPC2.Clients.HTTP

  # TODO: make configuration
  @url "http://localhost:81/"

  def publish_deleted_notifications(notifications) when is_list(notifications) do
    response = HTTP.call(@url, "publish:deleted_notifications", notifications)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end

  def publish_subscriptions(notifications) when is_list(notifications) do
    response = HTTP.call(@url, "publish:subscriptions", notifications)

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
    response = HTTP.call(@url, "publish:event_definition_ids", event_definition_ids)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end
end