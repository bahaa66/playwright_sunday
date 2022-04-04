defmodule CogyntWorkstationIngest.Servers.Workers.RedisStreamsConsumerGroupWorker do
  @moduledoc """
  Worker module that fetches messages of a Redis ConsumerGroup for the `itw` stream.
  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager

  @count 1

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
    Process.send_after(__MODULE__, :fetch_tasks, Config.ingest_task_worker_timer())
    {:ok, %{}}
  end

  @impl true
  def handle_info(:fetch_tasks, state) do
    case Redis.stream_read_group("itw", "cogynt-ws-ingest-otp", @count) do
      {:ok, stream_results} ->
        Enum.each(stream_results, fn [message_id, message_fields] ->
          Enum.each(message_fields, fn {field_name, field_value} ->
            case field_name do
              :backfill_notifications ->
                ConsumerStateManager.manage_request(%{backfill_notifications: field_value})

              :update_notifications ->
                ConsumerStateManager.manage_request(%{update_notifications: field_value})

              :delete_notifications ->
                ConsumerStateManager.manage_request(%{
                  delete_notifications: field_value
                })

              _ ->
                nil
            end
          end)

          # ACK the message as processed
          Redis.stream_ack_message("itw", message_id)
        end)

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to read from redis consumer group. reason: #{inspect(reason)}"
        )
    end

    Process.send_after(__MODULE__, :fetch_tasks, Config.ingest_task_worker_timer())
    {:noreply, state}
  end
end
