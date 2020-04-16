defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Broadway.Producer
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Events.EventDefinition

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
          body: "topic does not exist"
          # body: ConsumerStatusTypeEnum.status()[:topic_does_not_exist]
        }

      {:error, :failed_to_start_consumer} ->
        %{
          status: :error,
          body: :failed_to_start_consumer
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
      {:error, :failed_to_stop_consumer} ->
        %{
          status: :error,
          body: :failed_to_stop_consumer
        }

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

  def handle_request("ingest:check_status", consumers) when is_list(consumers) do
    try do
      # Grab a list of existing Kafka topics
      existing_topics = KafkaEx.metadata().topic_metadatas |> Enum.map(& &1.topic)

      result =
        Enum.reduce(consumers, [], fn %{"id" => id, "topic" => topic}, acc ->
          case Enum.member?(existing_topics, topic) do
            false ->
              acc ++
                [
                  %{
                    id: id,
                    topic: topic,
                    status: "topic does not exist"
                    # status: ConsumerStatusTypeEnum.status()[:topic_does_not_exist]
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
                            status: "has not been created"
                            # status: ConsumerStatusTypeEnum.status()[:has_not_been_created]
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
                                status: "is active, but no consumer running"
                                # status:
                                #   ConsumerStatusTypeEnum.status()[
                                #     :is_active_but_no_consumer_running
                                #   ]
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
                                    status: "paused"
                                    # status:
                                    #   ConsumerStatusTypeEnum.status()[:paused_and_processing]
                                  }
                                ]

                            false ->
                              acc ++
                                [
                                  %{
                                    id: id,
                                    topic: topic,
                                    status: "paused"
                                    # status: ConsumerStatusTypeEnum.status()[:paused_and_finished]
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
                        status: "running"
                        # status: ConsumerStatusTypeEnum.status()[:running]
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

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end

  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")
end
