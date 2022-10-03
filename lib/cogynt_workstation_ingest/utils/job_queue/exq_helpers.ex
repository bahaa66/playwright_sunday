defmodule CogyntWorkstationIngest.Utils.JobQueue.ExqHelpers do
  @default_concurrency 5

  def create_and_enqueue(
        queue_prefix,
        queue_id,
        worker,
        args,
        concurrency \\ @default_concurrency
      ) do
    case create_job_queue_if_not_exists(queue_prefix, queue_id, concurrency) do
      {:ok, queue_name} ->
        {:ok, _job_id} =
          Exq.enqueue(Exq, queue_name, worker, [
            args
          ])

        {:ok, :success}

      {:error, error} ->
        {:error, error}
    end
  end

  def create_job_queue_if_not_exists(queue_prefix, queue_id, concurrency \\ @default_concurrency)

  def create_job_queue_if_not_exists(queue_prefix, nil, concurrency) do
    case Exq.subscriptions(Exq) do
      {:ok, subscriptions} ->
        if !Enum.member?(subscriptions, queue_prefix) do
          Exq.subscribe(Exq, queue_prefix, concurrency)
          CogyntLogger.info("#{__MODULE__}", "Created Queue: #{queue_prefix}")
        end

        {:ok, queue_prefix}

      _ ->
        CogyntLogger.error("#{__MODULE__}", "Exq.Api.queues/1 failed to fetch queues")
        {:error, :failed_to_fetch_queues}
    end
  end

  def create_job_queue_if_not_exists(queue_prefix, queue_id, concurrency) do
    case Exq.subscriptions(Exq) do
      {:ok, subscriptions} ->
        queue_name = queue_prefix <> "-" <> "#{queue_id}"

        if !Enum.member?(subscriptions, queue_name) do
          Exq.subscribe(Exq, queue_name, concurrency)
          CogyntLogger.info("#{__MODULE__}", "Created Queue: #{queue_name}")
        end

        {:ok, queue_name}

      _ ->
        CogyntLogger.error("#{__MODULE__}", "Exq.Api.queues/1 failed to fetch queues")
        {:error, :failed_to_fetch_queues}
    end
  end

  def enqueue(queue_name, worker, args) do
    {:ok, _job_id} =
      Exq.enqueue(Exq, queue_name, worker, [
        args
      ])

    {:ok, :success}
  end

  def unubscribe_and_remove(queue_name) do
    Exq.unsubscribe(Exq, queue_name)
    Exq.Api.remove_queue(Exq.Api, queue_name)
  end

  def resubscribe_to_all_queues() do
    case Exq.Api.queues(Exq.Api) do
      {:ok, queues} ->
        Enum.each(queues, fn queue_name ->
          Exq.subscribe(Exq, queue_name, @default_concurrency)
        end)

      _ ->
        nil
    end
  end

  def queue_finished_processing?(queue_prefix, queue_id) do
    try do
      Enum.reduce([queue_prefix], true, fn prefix, acc ->
        queue_name = prefix <> "-" <> "#{queue_id}"
        {:ok, count} = Exq.Api.queue_size(Exq.Api, queue_name)
        # Exq.Api.processes returns a list of structs that looks like
        # [
        #   %Exq.Support.Process{
        #     host: "cogynt-ws-ingest-otp",
        #     payload: %Exq.Support.Job{
        #       args: ["7f3b93aa-bf67-47c3-88d2-c8bc4bf9b0d6"],
        #       class: "CogyntWorkstationIngest.Utils.JobQueue.Workers.BackfillNotificationsWorker",
        #       enqueued_at: 1_662_997_299.775067,
        #       error_class: nil,
        #       error_message: nil,
        #       failed_at: nil,
        #       finished_at: nil,
        #       jid: "f8c135e6-3b43-476a-8238-5e43720d2c2d",
        #       processor: nil,
        #       queue: "notifications-f71ef2d1-7cd3-54d8-87cf-4b8040595698",
        #       retried_at: nil,
        #       retry: 1,
        #       retry_count: nil
        #     },
        #     pid: "#PID<0.5838.0>",
        #     queue: "notifications-f71ef2d1-7cd3-54d8-87cf-4b8040595698",
        #     run_at: 1_662_997_299.786869
        #   }
        # ]
        {:ok, processes} = Exq.Api.processes(Exq.Api)

        grouped =
          Enum.group_by(processes, fn %Exq.Support.Process{queue: q} = _process ->
            q
          end)

        queue_processes = Map.get(grouped, queue_name, [])

        if count > 0 or Enum.count(queue_processes) > 1 do
          false
        else
          acc
        end
      end)
    rescue
      e ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "is_job_queue_finished?/1 failed with exception #{inspect(e)}"
        )

        true
    end
  end

  def flush_all() do
    try do
      Exq.Api.clear_processes(Exq.Api)
      Exq.Api.clear_failed(Exq.Api)
      Exq.Api.clear_retries(Exq.Api)
      Exq.Api.clear_scheduled(Exq.Api)

      case Exq.Api.queues(Exq.Api) do
        {:ok, queues} ->
          Enum.each(queues, fn queue_name ->
            Exq.unsubscribe(Exq, queue_name)
            Exq.Api.remove_queue(Exq.Api, queue_name)
          end)

        _ ->
          nil
      end
    rescue
      e ->
        CogyntLogger.error("#{__MODULE__}", "Failed to Reset JobQ data. Error: #{e}")
    end
  end
end
