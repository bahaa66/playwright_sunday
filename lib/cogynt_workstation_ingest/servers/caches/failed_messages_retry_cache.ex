defmodule CogyntWorkstationIngest.Servers.Caches.FailedMessagesRetryCache do
  @moduledoc """
  Module that will poll the failed pipeline messages from Redis and republish them back to each pipeline
  """
  use GenServer
  alias CogyntWorkstationIngest.Config

  @demand 100
  @deployment_pipeline_name :DeploymentPipeline

  # TODO: To be able to have High availablity pods this modules state needs to be populated
  # based on the value of the consumer status in the Startup task. Also need to check against
  # the consumer status before starting a consumer

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
    failed_deployment_messages = fetch_and_release_failed_messages(@demand, "fdpm")
    Broadway.push_messages(@deployment_pipeline_name, failed_deployment_messages)

    # poll for failed drilldown messages
    {:ok, drilldown_failed_message_keys} = Redis.keys_by_pattern("fdm:*")

    Enum.each(drilldown_failed_message_keys, fn key ->
      broadway_pipeline_name =
        String.split(key, ":")
        |> List.delete_at(0)
        |> Enum.join("Pipeline")
        |> String.to_atom()

      failed_drilldown_messages = fetch_and_release_failed_messages(@demand, key)
      Broadway.push_messages(broadway_pipeline_name, failed_drilldown_messages)
    end)

    # poll for failed event messages
    {:ok, event_definition_failed_message_keys} = Redis.keys_by_pattern("fem:*")

    Enum.each(event_definition_failed_message_keys, fn key ->
      broadway_pipeline_name =
        String.split(key, ":")
        |> List.delete_at(0)
        |> Enum.join("Pipeline")
        |> String.to_atom()

      failed_event_definition_messages = fetch_and_release_failed_messages(@demand, key)
      Broadway.push_messages(broadway_pipeline_name, failed_event_definition_messages)
    end)

    {:noreply, %{}}
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp fetch_and_release_failed_messages(demand, key) do
    {:ok, list_length} =
      case Redis.key_exists?(key) do
        {:ok, false} ->
          {:ok, 0}

        {:ok, true} ->
          Redis.list_length(key)
      end

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
  end
end
