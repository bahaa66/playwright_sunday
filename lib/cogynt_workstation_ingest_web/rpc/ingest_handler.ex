defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler

  alias CogyntWorkstationIngest.Supervisors.{
    ConsumerGroupSupervisor,
    EventSupervisor,
    LinkEventSupervisor
  }

  @linkage Application.get_env(:cogynt_workstation_ingest, :core_keys)[:link_data_type]

  def handle_request("start:consumer", event_definition) when is_map(event_definition) do
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

  def handle_request("stop:consumer", event_definition) when is_map(event_definition) do
    event_definition = keys_to_atoms(event_definition)

    with :ok <- ConsumerGroupSupervisor.stop_child(event_definition.topic),
         true <- link_event?(event_definition),
         :ok <- LinkEventSupervisor.stop_child(event_definition.topic) do
      %{
        status: :ok,
        body: :success
      }
    else
      false ->
        case EventSupervisor.stop_child(event_definition.topic) do
          :ok ->
            %{
              status: :ok,
              body: :success
            }

          {:error, error} ->
            %{
              status: :error,
              body: "#{inspect(error)}"
            }
        end

      {:error, error} ->
        %{
          status: :error,
          body: "#{inspect(error)}"
        }
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp link_event?(%{event_type: type}), do: type == @linkage

  defp keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end
end
