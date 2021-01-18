defmodule CogyntWorkstationIngest.Broadway.DrilldownPipeline do
  @moduledoc """
  Broadway pipeline module for the DrilldownPipeline. Defines the producer and
  processor configurations as well as the transform/2, ack/3 and handle_message/3
  methods
  """
  use Broadway
  alias Broadway.Message
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.DrilldownProcessor
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias Models.Deployments.Deployment

  def start_link(%{group_id: group_id, topics: topics, hosts: hosts}) do
    Broadway.start_link(__MODULE__,
      name: String.to_atom(group_id <> "Pipeline"),
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
        concurrency: Config.drilldown_producer_stages(),
        transformer: {__MODULE__, :transform, [group_id: group_id]}
      ],
      processors: [
        default: [
          concurrency: Config.drilldown_processor_stages()
        ]
      ],
      context: [group_id: group_id]
    )
  end

  @doc """
  Transformation callback. Will transform the message that is returned
  by the Producer into a Broadway.Message.t() to be handled by the processor
  """
  def transform(%Message{data: encoded_data} = message, opts) do
    group_id = Keyword.get(opts, :group_id, 1)

    case Jason.decode(encoded_data) do
      {:ok, decoded_data} ->
        # Incr the total message count that has been consumed from kafka
        incr_total_fetched_message_count(group_id)

        Map.put(message, :data, %{event: decoded_data, retry_count: 0})

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode DrilldownPipeline Kafka message. Error: #{inspect(error)}"
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
  Callback for handling any failed messages in the EventPipeline. It will
  take the failed messages and queue them back on the producer to get tried
  again.
  """
  @impl true
  def handle_failed(messages, context) do
    group_id = Keyword.get(context, :group_id, 1)

    incr_total_processed_message_count(group_id, Enum.count(messages))

    failed_messages =
      Enum.reduce(messages, [], fn %Broadway.Message{data: %{retry_count: retry_count} = data} =
                                     message,
                                   acc ->
        if retry_count < Config.failed_messages_max_retry() do
          new_retry_count = retry_count + 1

          CogyntLogger.warn(
            "#{__MODULE__}",
            "Retrying Failed DrilldownPipeline Message. Attempt: #{new_retry_count}"
          )

          data = Map.put(data, :retry_count, new_retry_count)

          message =
            Map.from_struct(message)
            |> Map.put(:data, data)
            |> Map.drop([:status, :acknowledger])

          metadata =
            Map.get(message, :metadata)
            |> Map.put(:key, "")

          message = Map.put(message, :metadata, metadata)

          acc ++ [message]
        else
          acc
        end
      end)

    Redis.list_append_pipeline("fdm:#{group_id}", failed_messages)
    messages
  end

  @doc """
  Handle_message callback. Takes the Broadway.Message.t() from the
  transform callback and processes the data object. Runs the data through
  a process_template_data/1 and update_cache/1
  """
  @impl true
  def handle_message(_processor, message, context) do
    group_id = Keyword.get(context, :group_id, 1)

    message
    |> DrilldownProcessor.process_template_data()
    |> DrilldownProcessor.upsert_template_solutions()

    incr_total_processed_message_count(group_id)
    message
  end

  @doc false
  def drilldown_pipeline_running?(deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        consumer_group_id = ConsumerGroupSupervisor.fetch_drilldown_cgid()

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end

      %Deployment{id: deployment_id} ->
        consumer_group_id = ConsumerGroupSupervisor.fetch_drilldown_cgid(deployment_id)

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end
    end
  end

  @doc false
  def drilldown_pipeline_finished_processing?(deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        consumer_group_id = ConsumerGroupSupervisor.fetch_drilldown_cgid()

        if consumer_group_id == "" do
          true
        else
          case Redis.key_exists?("dmi:#{consumer_group_id}") do
            {:ok, false} ->
              true

            {:ok, true} ->
              {:ok, tmc} = Redis.hash_get("dmi:#{consumer_group_id}", "tmc")
              {:ok, tmp} = Redis.hash_get("dmi:#{consumer_group_id}", "tmp")

              String.to_integer(tmp) >= String.to_integer(tmc)
          end
        end

      %Deployment{id: deployment_id} ->
        consumer_group_id = ConsumerGroupSupervisor.fetch_drilldown_cgid(deployment_id)

        if consumer_group_id == "" do
          true
        else
          case Redis.key_exists?("dmi:#{consumer_group_id}") do
            {:ok, false} ->
              true

            {:ok, true} ->
              {:ok, tmc} = Redis.hash_get("dmi:#{consumer_group_id}", "tmc")
              {:ok, tmp} = Redis.hash_get("dmi:#{consumer_group_id}", "tmp")

              String.to_integer(tmp) >= String.to_integer(tmc)
          end
        end
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp incr_total_fetched_message_count(group_id) do
    Redis.hash_increment_by("dmi:#{group_id}", "tmc", 1)
    Redis.key_pexpire("dmi:#{group_id}", 10000)
  end

  defp incr_total_processed_message_count(group_id, count \\ 1) do
    Redis.hash_increment_by("dmi:#{group_id}", "tmp", count)
    Redis.key_pexpire("dmi:#{group_id}", 10000)
  end
end
