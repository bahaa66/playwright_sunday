defmodule CogyntWorkstationIngest.Broadway.DeploymentPipeline do
  @moduledoc """
  Broadway pipeline module for the DeploymentPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Broadway.DeploymentProcessor

  def start_link(%{group_id: group_id, topics: topics, hosts: hosts}) do
    Broadway.start_link(__MODULE__,
      name: :DeploymentPipeline,
      producer: [
        module:
          {BroadwayKafka.Producer,
           [
             hosts: hosts,
             group_id: group_id,
             topics: topics,
             offset_commit_on_ack: true,
             offset_reset_policy: :earliest,
             group_config: [
               session_timeout_seconds: 30,
               rejoin_delay_seconds: 10
             ],
             client_config: [
               connect_timeout: 30000
             ]
           ]},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [
          concurrency: 10
        ]
      ]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, _opts) do
    case Jason.decode(encoded_data, keys: :atoms) do
      {:ok, decoded_data} ->
        # Incr the total message count that has been consumed from kafka
        incr_total_fetched_message_count()

        # Store the deployment message and an initial retry count in the :data field of the message
        Map.put(message, :data, %{deployment_message: decoded_data, retry_count: 0})

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode DeploymentPipeline Kafka message. Error: #{inspect(error)}"
        )

        Map.put(message, :data, nil)
    end
  end

  @doc """
  Acknowledgment callback only triggered for when failed messages are republished
  through the pipeline
  """
  def ack(:ack_id, _successful, _failed) do
    :ok
  end

  @doc """
  Callback for handling any failed messages in the DeploymentPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, _opts) do
    # incr_total_processed_message_count(Enum.count(messages))

    # failed_messages =
    #   Enum.reduce(messages, [], fn %Broadway.Message{data: %{retry_count: retry_count} = data} =
    #                                  message,
    #                                acc ->
    #     if retry_count < Config.failed_messages_max_retry() do
    #       CogyntLogger.warn(
    #         "#{__MODULE__}",
    #         "Retrying Failed DeploymentPipeline Message. Attempt: #{retry_count + 1}"
    #       )

    #       data = Map.put(data, :retry_count, retry_count + 1)

    #       message =
    #         Map.from_struct(message)
    #         |> Map.put(:data, data)
    #         |> Map.drop([:status, :acknowledger])

    #       metadata =
    #         Map.get(message, :metadata)
    #         |> Map.put(:key, "")

    #       message = Map.put(message, :metadata, metadata)

    #       acc ++ [message]
    #     else
    #       acc
    #     end
    #   end)

    # case Redis.list_length("fdpm") do
    #   {:ok, length} when length >= 50_000 ->
    #     CogyntLogger.error(
    #       "#{__MODULE__}",
    #       "Failed Deployments messages have reached the limit of 50_000 in Redis. Dropping future messages from getting queued"
    #     )

    #   _ ->
    #     Redis.list_append_pipeline("fdpm", failed_messages)
    #     # 30 min TTL
    #     Redis.key_pexpire("fdpm", 1_800_000)
    # end

    CogyntLogger.warn(
      "#{__MODULE__}",
      "handle_failed/2 #{Enum.count(messages)} Failed for DeploymentPipeline. Check logs to for error."
    )

    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_template_data/1 and update_cache/1
  """
  @impl true
  def handle_message(_processor, message, _context) do
    message
    |> DeploymentProcessor.process_deployment_message()

    incr_total_processed_message_count()
    message
  end

  @doc false
  def pipeline_started?() do
    Process.whereis(:DeploymentPipeline)
    |> case do
      nil ->
        false

      _ ->
        true
    end
  end

  @doc false
  def pipeline_running?() do
    if pipeline_started?() do
      try do
        producer =
          Broadway.producer_names(:DeploymentPipeline)
          |> List.first()

        case GenStage.call(producer, :"$demand", 120_000) do
          :forward ->
            true

          :accumulate ->
            false
        end
      rescue
        _ ->
          false
      end
    else
      false
    end
  end

  @doc false
  # TODO: look into GenStage.estimate_buffered_count(stage, timeout \\ 5000)
  # to see if we can replace the redis message_info key with this.
  def pipeline_finished_processing?() do
    case Redis.key_exists?("dpmi") do
      {:ok, false} ->
        true

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("dpmi", "tmc")
        {:ok, tmp} = Redis.hash_get("dpmi", "tmp")

        String.to_integer(tmp) >= String.to_integer(tmc)
    end
  end

  @doc false
  def suspend_pipeline() do
    Broadway.producer_names(:DeploymentPipeline)
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :accumulate)
    end)
  end

  @doc false
  def resume_pipeline() do
    Broadway.producer_names(:DeploymentPipeline)
    |> Enum.each(fn producer ->
      GenStage.demand(producer, :forward)
    end)
  end

  @doc false
  def estimated_buffer_count() do
    Broadway.producer_names(:DeploymentPipeline)
    |> Enum.reduce(0, fn producer, acc ->
      acc + GenStage.estimate_buffered_count(producer)
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp incr_total_fetched_message_count() do
    Redis.hash_increment_by("dpmi", "tmc", 1)
    Redis.key_pexpire("dpmi", 60000)
  end

  defp incr_total_processed_message_count(count \\ 1) do
    Redis.hash_increment_by("dpmi", "tmp", count)
    Redis.key_pexpire("dpmi", 60000)
  end
end
