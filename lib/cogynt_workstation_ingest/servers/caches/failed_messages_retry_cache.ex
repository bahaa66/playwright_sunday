defmodule CogyntWorkstationIngest.Servers.Caches.FailedMessagesRetryCache do
  @moduledoc """
  Module that will poll the failed pipeline messages from Redis and republish them back to each pipeline
  """
  # TODO: move this module to the Workers folder and rename to FailedMessageRetryWorker
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

  @demand 100
  @deployment_pipeline_name :DeploymentPipeline
  @deployment_pipeline_module CogyntWorkstationIngest.Broadway.DeploymentPipeline
  @drilldown_pipeline_module CogyntWorkstationIngest.Broadway.DrilldownPipeline
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

    # poll for failed deployment messages
    failed_deployment_messages =
      fetch_and_release_failed_messages(@demand, @deployment_pipeline_module, "fdpm")

    Broadway.push_messages(@deployment_pipeline_name, failed_deployment_messages)

    # poll for failed drilldown messages
    {:ok, drilldown_failed_message_keys} = Redis.keys_by_pattern("fdm:*")

    Enum.each(drilldown_failed_message_keys, fn key ->
      broadway_pipeline_id =
        String.split(key, ":")
        |> List.last()

      failed_drilldown_messages =
        fetch_and_release_failed_messages(@demand, @drilldown_pipeline_module, key)

      Broadway.push_messages(
        String.to_atom(broadway_pipeline_id <> "Pipeline"),
        failed_drilldown_messages
      )
    end)

    # poll for failed event messages
    {:ok, event_definition_failed_message_keys} = Redis.keys_by_pattern("fem:*")

    Enum.each(event_definition_failed_message_keys, fn key ->
      event_definition_id =
        String.split(key, ":")
        |> List.last()

      consumer_group_id = ConsumerGroupSupervisor.fetch_event_cgid(event_definition_id)

      failed_event_definition_messages =
        fetch_and_release_failed_messages(@demand, @event_pipeline_module, key)

      Broadway.push_messages(
        String.to_atom(consumer_group_id <> "Pipeline"),
        failed_event_definition_messages
      )
    end)

    {:noreply, %{}}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp fetch_and_release_failed_messages(demand, pipeline_module, key) do
    {:ok, list_length} =
      case Redis.key_exists?(key) do
        {:ok, false} ->
          {:ok, 0}

        {:ok, true} ->
          Redis.list_length(key)
      end

    list_items =
      case list_length >= demand do
        true ->
          # Get List Range by demand
          {:ok, list_items} = Redis.list_range(key, 0, demand - 1)

          # Trim List Range by demand
          Redis.list_trim(key, demand, 100_000_000)

          list_items

        false ->
          if list_length > 0 do
            # Get List Range by list_length
            {:ok, list_items} = Redis.list_range(key, 0, list_length - 1)

            # Trim List Range by list_length
            Redis.list_trim(key, list_length, -1)

            list_items
          else
            []
          end
      end

    if not Enum.empty?(list_items) do
      Enum.reduce(list_items, [], fn json_encoded_message, acc ->
        case Jason.decode(json_encoded_message, keys: :atoms) do
          {:ok,
           %{
             batch_key: batch_key,
             data: data,
             metadata: metadata
           }} ->
            acc ++
              [
                %Broadway.Message{
                  acknowledger: {pipeline_module, :ack_id, :ack_data},
                  batch_key: list_to_tuple(batch_key),
                  batch_mode: :bulk,
                  batcher: :default,
                  data: data,
                  metadata: metadata
                }
              ]

          {:error, error} ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Failed to jason decode failed message for Redis key: #{key}. Error: #{
                inspect(error, pretty: true)
              }"
            )

            acc
        end
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
end
