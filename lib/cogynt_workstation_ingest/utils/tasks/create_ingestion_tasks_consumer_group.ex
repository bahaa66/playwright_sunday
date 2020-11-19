defmodule CogyntWorkstationIngest.Utils.Tasks.CreateIngestionTasksConsumerGroup do
  @moduledoc """
  Task that creates the consumer group and stream `itw` that is used for communication with ws-otp application
  """
  use Task

  def start_link(_arg \\ []) do
    Task.start_link(__MODULE__, :run, [])
  end

  def run() do
    case Redis.stream_create_group("itw", create_stream: true) do
      {:error, reason} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Failed to create ingestion_task_worker consumer_group. Reason: #{inspect(reason)}"
        )

        Process.sleep(1000)
        run()

      _ ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Success creating ingestion_task_worker consumer_group"
        )
    end
  end
end
