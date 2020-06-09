defmodule CogyntWorkstationIngestWeb.Rpc.CogyntClient do
  alias JSONRPC2.Clients.HTTP

  alias CogyntWorkstationIngest.Config
  alias Models.Notifications.Notification

  @path "/rpc/cogynt"

  # ----------------------------- #
  # --- publish notifications --- #
  # ----------------------------- #
  def publish_notifications(notifications) when is_list(notifications) do
    case Enum.empty?(notifications) do
      false ->
        url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"

        response =
          HTTP.call(
            url,
            "publish:subscriptions",
            notification_struct_to_map(notifications),
            [{"content-type", "application/json"}],
            :post,
            recv_timeout: 10000
          )

        case response do
          {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
            {:ok, body}

          {:ok, %{"body" => body, "status" => status}} when status == "error" ->
            {:error, body}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "publish_notifications failed with Error: #{inspect(error)}"
            )

            {:error, :internal_server_error}
        end

      true ->
        {:error, :empty_list_passed}
    end
  end

  def publish_updated_notifications(updated_notifications) when is_list(updated_notifications) do
    case Enum.empty?(updated_notifications) do
      false ->
        url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"

        response =
          HTTP.call(
            url,
            "publish:subscriptions",
            updated_notifications,
            [{"content-type", "application/json"}],
            :post,
            recv_timeout: 10000
          )

        case response do
          {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
            {:ok, body}

          {:ok, %{"body" => body, "status" => status}} when status == "error" ->
            {:error, body}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "publish_updated_notifications failed with Error: #{inspect(error)}"
            )

            {:error, :internal_server_error}
        end

      true ->
        {:error, :empty_list_passed}
    end
  end

  # ---------------------------- #
  # --- publish event counts --- #
  # ---------------------------- #
  def publish_event_definition_ids(event_definition_ids) when is_list(event_definition_ids) do
    url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"
    response = HTTP.call(url, "publish:event_definition_ids", event_definition_ids)

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "publish_event_definition_ids failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  # ------------------------------- #
  # --- publish consumer status --- #
  # ------------------------------- #
  def publish_consumer_status(id, topic, status) do
    request = %{
      id: id,
      topic: topic,
      status: status
    }

    url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"

    response =
      HTTP.call(
        url,
        "publish:consumer_status",
        request,
        [{"content-type", "application/json"}],
        :post,
        recv_timeout: 10000
      )

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "publish_consumer_status failed with Error: #{inspect(error)}"
        )

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

    url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"

    response =
      HTTP.call(
        url,
        "publish:notification_task_status",
        request,
        [{"content-type", "application/json"}],
        :post,
        recv_timeout: 10000
      )

    case response do
      {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
        {:ok, body}

      {:ok, %{"body" => body, "status" => status}} when status == "error" ->
        {:error, body}

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "publish_notification_task_status failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp notification_struct_to_map(notifications) do
    Enum.reduce(notifications, [], fn %Notification{} = notification, acc ->
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
end
