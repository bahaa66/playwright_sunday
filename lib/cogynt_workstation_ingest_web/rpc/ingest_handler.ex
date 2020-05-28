defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler
  use Task

  alias CogyntWorkstationIngest.Servers.NotificationsTaskMonitor
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Broadway.Producer
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Events.EventDefinition

  # ----------------------- #
  # --- ingestion calls --- #
  # ----------------------- #
  def handle_request("ingest:start_consumer", event_definition) when is_map(event_definition) do
    result = ConsumerGroupSupervisor.start_child(keys_to_atoms(event_definition))

    case result do
      {:ok, pid} ->
        %{
          status: :ok,
          body: "#{inspect(pid)}"
        }

      {:error, nil} ->
        %{
          status: :error,
          body: ConsumerStatusTypeEnum.status()[:topic_does_not_exist]
        }

      {:error, error} ->
        %{
          status: :error,
          body: "#{inspect(error)}"
        }
    end
  end

  def handle_request("ingest:stop_consumer", event_definition) when is_map(event_definition) do
    event_definition = keys_to_atoms(event_definition)

    with {:ok, :success} <- ConsumerGroupSupervisor.stop_child(event_definition.topic) do
      %{
        status: :ok,
        body: :success
      }
    else
      {:error, error} ->
        %{
          status: :error,
          body: "#{inspect(error)}"
        }
    end
  end

  def handle_request("ingest:backfill_notifications", %{
        "notification_setting_id" => notification_setting_id
      }) do
    TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:update_notifications", %{
        "notification_setting_id" => notification_setting_id
      }) do
    TaskSupervisor.start_child(%{update_notification_setting: notification_setting_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:delete_event_definition_events", %{
        "event_definition_id" => event_definition_id
      }) do
    TaskSupervisor.start_child(%{delete_event_definition_events: event_definition_id})

    %{
      status: :ok,
      body: :success
    }
  end

  def handle_request("ingest:check_status", consumers) when is_list(consumers) do
    try do
      # TODO: Temp
      KafkaEx.create_worker(:standard,
        consumer_group: "kafka_ex",
        consumer_group_update_interval: 100
      )

      # Grab a list of existing Kafka topics
      existing_topics =
        KafkaEx.metadata(worker_name: :standard).topic_metadatas |> Enum.map(& &1.topic)

      result =
        Enum.reduce(consumers, [], fn %{"id" => id, "topic" => topic}, acc ->
          case Enum.member?(existing_topics, topic) do
            false ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist]
                  }
                ]

            true ->
              case Process.whereis(consumer_group_name(topic)) do
                nil ->
                  case EventsContext.get_event_definition(id) do
                    nil ->
                      acc ++
                        [
                          %{
                            id: id,
                            topic: topic,
                            status: ConsumerStatusTypeEnum.status()[:has_not_been_created]
                          }
                        ]

                    %EventDefinition{} = event_definition ->
                      case event_definition.active do
                        true ->
                          acc ++
                            [
                              %{
                                id: id,
                                topic: topic,
                                status:
                                  ConsumerStatusTypeEnum.status()[
                                    :is_active_but_no_consumer_running
                                  ]
                              }
                            ]

                        false ->
                          case Producer.is_processing?(id, event_definition.event_type) do
                            true ->
                              acc ++
                                [
                                  %{
                                    id: id,
                                    topic: topic,
                                    status:
                                      ConsumerStatusTypeEnum.status()[:paused_and_processing]
                                  }
                                ]

                            false ->
                              acc ++
                                [
                                  %{
                                    id: id,
                                    topic: topic,
                                    status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
                                  }
                                ]
                          end
                      end
                  end

                _ ->
                  acc ++
                    [
                      %{
                        id: id,
                        topic: topic,
                        status: ConsumerStatusTypeEnum.status()[:running]
                      }
                    ]
              end
          end
        end)

      %{
        status: :ok,
        body: result
      }
    rescue
      _ ->
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
        CogyntLogger.error("#{__MODULE__}", "dev_delete failed with error: #{inspect(error)}")

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
      _ ->
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
