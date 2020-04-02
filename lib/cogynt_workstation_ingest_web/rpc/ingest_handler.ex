defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  # alias CogyntWorkstationIngest.Broadway.Producer

  # @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]

  def handle_request("ingest:start_consumer", event_definition) when is_map(event_definition) do
    result = ConsumerGroupSupervisor.start_child(keys_to_atoms(event_definition))

    case result do
      {:ok, nil} ->
        %{
          status: :error,
          body: :topic_does_not_exist
        }

      {:ok, pid} ->
        %{
          status: :ok,
          body: "#{inspect(pid)}"
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

    with :ok <- ConsumerGroupSupervisor.stop_child(event_definition.topic) do
      # true <- link_event?(event_definition),
      # :ok <- Producer.drain_queue(event_definition.id, :linkevent) do
      %{
        status: :ok,
        body: :success
      }
    else
      # false ->
      #   case Producer.drain_queue(event_definition.id, :event) do
      #     :ok ->
      #       %{
      #         status: :ok,
      #         body: :success
      #       }

      #     {:error, error} ->
      #       %{
      #         status: :error,
      #         body: "#{inspect(error)}"
      #       }
      #   end

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
      # reduce the consumer list
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
                  }
                ]

            true ->
              case Process.whereis(consumer_group_name(topic)) do
                nil ->
                  event_definition =
                    EventsContext.get_event_definition_by(%{id: id, deleted_at: nil})

                  case event_definition.active do
                    nil ->
                      acc ++
                        [
                          %{
                            id: id,
                            topic: topic,
                            status: "has not been created yet"
                          }
                        ]

                    false ->
                      acc ++
                        [
                          %{
                            id: id,
                            topic: topic,
                            status: "paused"
                          }
                        ]

                    true ->
                      acc ++
                        [
                          %{
                            id: id,
                            topic: topic,
                            status: "is active, but no consumer running"
                          }
                        ]
                  end

                _ ->
                  acc ++
                    [
                      %{
                        id: id,
                        topic: topic,
                        status: "running"
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
  # defp link_event?(%{event_type: type}), do: type == @linkage

  defp keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end

  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")
end
