defmodule CogyntWorkstationIngest.Servers.Workers.FailedMessagesRetryWorker do
  @moduledoc """
  Module that will poll the failed pipeline messages from Redis and republish them back to each pipeline
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Broadway.{EventPipeline, DeploymentPipeline}

  @demand 5000
  @deployment_pipeline_name :DeploymentPipeline
  @deployment_pipeline_module CogyntWorkstationIngest.Broadway.DeploymentPipeline
  @event_pipeline_module CogyntWorkstationIngest.Broadway.EventPipeline

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    Process.send_after(
      __MODULE__,
      :process_failed_messages,
      Config.failed_messages_retry_timer()
    )

    {:ok, %{}}
  end

  @impl true
  def handle_info(:process_failed_messages, _state) do
    Process.send_after(
      __MODULE__,
      :process_failed_messages,
      Config.failed_messages_retry_timer()
    )

    try do
      # poll for failed deployment messages
      if DeploymentPipeline.deployment_pipeline_running?() do
        failed_deployment_messages =
          fetch_and_release_failed_messages(@demand, @deployment_pipeline_module, "fdpm")

        Broadway.push_messages(@deployment_pipeline_name, failed_deployment_messages)
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to push failed_messages to DeploymentPipeline. Error: #{inspect(error)}"
        )
    end

    try do
      # poll for failed event messages
      {:ok, event_definition_failed_message_keys} = Redis.keys_by_pattern("fem:*")

      Enum.each(event_definition_failed_message_keys, fn key ->
        event_definition_id =
          String.split(key, ":")
          |> List.last()

        consumer_group_id = ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id)

        if EventPipeline.event_pipeline_running?(event_definition_id) do
          failed_event_definition_messages =
            fetch_and_release_failed_messages(@demand, @event_pipeline_module, key)

          Broadway.push_messages(
            String.to_atom(consumer_group_id <> "Pipeline"),
            failed_event_definition_messages
          )
        end
      end)
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to push failed_messages to EventPipeline. Error: #{inspect(error)}"
        )
    end

    {:noreply, %{}}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp fetch_and_release_failed_messages(demand, pipeline_module, key) do
    {:ok, list_length} = Redis.list_length(key)

    list_items =
      case list_length >= demand do
        true ->
          # Get List Range by demand
          {:ok, list_items} = Redis.list_range(key, 0, demand - 1)

          # Trim List Range by demand
          Redis.list_trim(key, demand, 100_000_000)
          # 30 min TTL
          Redis.key_pexpire(key, 1_800_000)

          list_items

        false ->
          if list_length > 0 do
            # Get List Range by list_length
            {:ok, list_items} = Redis.list_range(key, 0, list_length - 1)

            # Trim List Range by list_length
            Redis.list_trim(key, list_length, -1)
            # 30 min TTL
            Redis.key_pexpire(key, 1_800_000)

            list_items
          else
            []
          end
      end

    if not Enum.empty?(list_items) do
      Enum.reduce(list_items, [], fn %{
                                       batch_key: batch_key,
                                       batcher: batcher,
                                       batch_mode: batch_mode,
                                       data: data,
                                       metadata: metadata
                                     },
                                     acc ->
        acc ++
          [
            %Broadway.Message{
              acknowledger: {pipeline_module, :ack_id, :ack_data},
              batch_key: list_to_tuple(batch_key),
              batch_mode: String.to_atom(batch_mode),
              batcher: String.to_atom(batcher),
              data: build_atom_key_map(data),
              metadata: build_atom_key_map(metadata)
            }
          ]
      end)
    else
      list_items
    end
  end

  defp list_to_tuple(list) when is_list(list) do
    Enum.reduce(list, {}, fn item, acc ->
      Tuple.append(acc, list_to_tuple(item))
    end)
  end

  defp list_to_tuple(item) do
    item
  end

  defp build_atom_key_map(string_key_map) do
    for {key, val} <- string_key_map, into: %{}, do: pipeline_key_conditions(key, val)
  end

  defp convert_keys_to_atoms(list_of_maps) when is_list(list_of_maps) do
    Enum.reduce(list_of_maps, [], fn string_map, acc ->
      acc ++ [convert_keys_to_atoms(string_map)]
    end)
  end

  defp convert_keys_to_atoms(string_key_map) when is_map(string_key_map) do
    for {key, val} <- string_key_map,
        into: %{},
        do: {String.to_atom(key), convert_keys_to_atoms(val)}
  end

  defp convert_keys_to_atoms(value), do: value

  defp pipeline_key_conditions(key, val) do
    case(key) do
      "deployment_message" ->
        {String.to_atom(key), convert_keys_to_atoms(val)}

      "event_definition" ->
        {String.to_atom(key), convert_keys_to_atoms(val)}

      _ ->
        {String.to_atom(key), val}
    end
  end
end
