defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler
  use Task

  alias CogyntWorkstationIngest.Servers.NotificationsTaskMonitor
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Broadway.Producer
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.Servers.ConsumerStateManager

  # ----------------------- #
  # --- ingestion calls --- #
  # ----------------------- #
  def handle_request("ingest:start_consumer", event_definition) when is_map(event_definition) do
    result =
      ConsumerStateManager.manage_request(%{start_consumer: keys_to_atoms(event_definition)})

    case result do
      {:ok, _pid} ->
        %{
          status: :ok,
          body: :consumer_started
        }

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "ingest:start_consumer failed with Error: #{inspect(error)}"
        )

        %{
          status: :error,
          body: "#{inspect(error)}"
        }
    end
  end

  def handle_request("ingest:stop_consumer", event_definition) when is_map(event_definition) do
    event_definition = keys_to_atoms(event_definition)

    case ConsumerStateManager.manage_request(%{stop_consumer: event_definition.topic}) do
      {:ok, status} ->
        %{
          status: :ok,
          body: status
        }

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "ingest:stop_consumer failed with Error: #{inspect(error)}"
        )

        %{
          status: :error,
          body: "#{inspect(error)}"
        }
    end
  end

  def handle_request("ingest:backfill_notifications", %{
        "notification_setting_id" => notification_setting_id
      }) do
    ConsumerStateManager.manage_request(%{backfill_notifications: notification_setting_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:update_notifications", %{
        "notification_setting_id" => notification_setting_id
      }) do
    ConsumerStateManager.manage_request(%{update_notification_setting: notification_setting_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:delete_event_definition_events", %{
        "event_definition_id" => event_definition_id
      }) do
    ConsumerStateManager.manage_request(%{delete_event_definition_events: event_definition_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:check_status", consumers) when is_list(consumers) do
    try do
      result =
        Enum.reduce(consumers, [], fn %{"id" => id, "topic" => topic}, acc ->
          consumer_state = ConsumerStateManager.get_consumer_state(id)

          cond do
            consumer_state == %{} ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: ConsumerStatusTypeEnum.status()[:has_not_been_created]
                  }
                ]

            consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: consumer_state.prev_status
                  }
                ]

            consumer_state.status ==
                ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: consumer_state.prev_status
                  }
                ]

            true ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: consumer_state.status
                  }
                ]
          end
        end)

      %{
        status: :ok,
        body: result
      }
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "ingest:check_status failed with Error: #{inspect(error)}"
        )

        %{
          status: :error,
          body: :internal_server_error
        }
    end
  end

  def handle_request("ingest:dev_delete", %{
        "drilldown" => reset_drilldown,
        "event_definition_ids" => event_definition_ids,
        "topics" => delete_topics
      }) do
    try do
      if reset_drilldown do
        TaskSupervisor.start_child(%{delete_drilldown_data: delete_topics})
      end

      if length(event_definition_ids) > 0 do
        TaskSupervisor.start_child(%{
          delete_topic_data: %{
            event_definition_ids: event_definition_ids,
            delete_topics: delete_topics
          }
        })

        %{
          status: :ok,
          body: :processing
        }
      else
        %{
          status: :ok,
          body: :nothing_to_process
        }
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "ingest:dev_delete failed with error: #{inspect(error)}"
        )

        %{
          status: :error,
          body: :internal_server_error
        }
    end
  end

  # --------------------------- #
  # --- notifications tasks --- #
  # --------------------------- #
  def handle_request("notifications:check_status", notification_setting_ids)
      when is_list(notification_setting_ids) do
    try do
      response =
        Enum.reduce(notification_setting_ids, [], fn id, acc ->
          case NotificationsTaskMonitor.is_processing?(id) do
            true ->
              acc ++
                [
                  %{
                    id: id,
                    status: :running
                  }
                ]

            false ->
              acc ++
                [
                  %{
                    id: id,
                    status: :finished
                  }
                ]
          end
        end)

      %{
        status: :ok,
        body: response
      }
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "notifications:check_status failed with error: #{inspect(error)}"
        )

        %{
          status: :error,
          body: :internal_server_error
        }
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end

  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")
end
