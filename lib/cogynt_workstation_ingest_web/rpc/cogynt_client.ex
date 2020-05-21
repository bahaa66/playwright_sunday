defmodule CogyntWorkstationIngestWeb.Rpc.CogyntClient do
  alias JSONRPC2.Clients.HTTP

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum
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
          HTTP.call(url, "publish:subscriptions", notification_struct_to_map(notifications))

        case response do
          {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
            {:ok, body}

          {:ok, %{"body" => body, "status" => status}} when status == "error" ->
            {:error, body}

          {:error, _} ->
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

        response = HTTP.call(url, "publish:subscriptions", updated_notifications)

        case response do
          {:ok, %{"body" => body, "status" => status}} when status == "ok" ->
            {:ok, body}

          {:ok, %{"body" => body, "status" => status}} when status == "error" ->
            {:error, body}

          {:error, _} ->
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
      CogyntLogger.warn("#{__MODULE__}", "Publishing consumer status for ID: #{id}")

      request = %{
        id: id,
        topic: event_definition.topic,
        status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
      }

      url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"
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
        CogyntLogger.warn("#{__MODULE__}", "Publishing consumer status failed for ID: #{id}")
        {:error, :event_definition_does_not_exist}

      true ->
        CogyntLogger.warn("#{__MODULE__}", "Publishing consumer status failed for ID: #{id}")
        {:error, :event_definition_is_active}
    end
  end

  def publish_consumer_status(id, topic, status) do
    request = %{
      id: id,
      topic: topic,
      status: status
    }

    url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"
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

    url = "#{Config.cogynt_otp_service_name()}:#{Config.cogynt_otp_service_port()}#{@path}"
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
