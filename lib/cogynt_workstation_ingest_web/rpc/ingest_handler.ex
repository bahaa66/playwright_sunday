defmodule CogyntWorkstationIngestWeb.Rpc.IngestHandler do
  use JSONRPC2.Server.Handler

  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  def handle_request("start:consumer", event_definition) when is_map(event_definition) do
    {:ok, pid} = ConsumerGroupSupervisor.start_child(keys_to_atoms(event_definition))

    case pid do
      nil ->
        %{
          status: :error,
          body: :topic_does_not_exist
        }

      pid ->
        %{
          status: :ok,
          body: "#{inspect pid}"
        }
    end
  end

  def handle_request("stop:consumer", event_definition) when is_map(event_definition) do
    event_definition = keys_to_atoms(event_definition)
    result = ConsumerGroupSupervisor.stop_children(event_definition.topic)

    case result do
      {:error, error} ->
        %{
          status: :error,
          body: "#{inspect error}"
        }

      :ok ->
        %{
          status: :ok,
          body: :success
        }
    end
  end

  defp keys_to_atoms(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: {String.to_atom(key), val}
  end
end
