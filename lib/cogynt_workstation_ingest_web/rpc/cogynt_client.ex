defmodule CogyntWorkstationIngestWeb.Rpc.CogyntClient do
  alias JSONRPC2.Clients.HTTP

  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  @path "/rpc/cogynt"

  # ----------------------------- #
  # --- publish notifications --- #
  # ----------------------------- #
  def publish_deleted_notifications(notifications) when is_list(notifications) do
    url = "#{service_name()}:#{service_port()}#{@path}"
    response = HTTP.call(url, "publish:deleted_notifications", notifications)

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

  # ---------------------------- #
  # --- publish event counts --- #
  # ---------------------------- #
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

  # ------------------------------- #
  # --- publish consumer status --- #
  # ------------------------------- #
  def publish_consumer_status(id, nil) do
    with %EventDefinition{} = event_definition <- EventsContext.get_event_definition(id),
         false <- event_definition.active do
      request = %{
        id: id,
        topic: event_definition.topic,
        status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
      }

      url = "#{service_name()}:#{service_port()}#{@path}"
      response = HTTP.call(url, "publish:consumer_status", request)

      case response do
        {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
          {:ok, body}

        {:ok, %{"body" => body, "status" => status}} when status == "error" ->
          {:error, body}

        {:error, _} ->
          {:error, :internal_server_error}
      end
    else
      nil ->
        {:error, :event_definition_does_not_exist}

      true ->
        {:error, :event_definition_is_active}
    end
  end

  def publish_consumer_status(id, topic, status) do
    request = %{
      id: id,
      topic: topic,
      status: status
    }

    url = "#{service_name()}:#{service_port()}#{@path}"
    response = HTTP.call(url, "publish:consumer_status", request)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, _} ->
        {:error, :internal_server_error}
    end
  end

  # ---------------------------------------- #
  # --- publish notification task status --- #
  # ---------------------------------------- #
  def publish_notification_task_status(notification_setting_id, status) do
    request = %{
      id: notification_setting_id,
      status: status
    }

    url = "#{service_name()}:#{service_port()}#{@path}"
    response = HTTP.call(url, "publish:notification_task_status", request)

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
