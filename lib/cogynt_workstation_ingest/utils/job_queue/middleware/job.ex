defmodule CogyntWorkstationIngest.Utils.JobQueue.Middleware.Job do
  @behaviour Exq.Middleware.Behaviour
  alias Exq.Redis.JobQueue
  alias Exq.Middleware.Pipeline
  import Pipeline

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    BackfillNotificationsWorker,
    UpdateNotificationsWorker,
    DeleteNotificationsWorker,
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  @template_solutions_id 1
  @template_solution_events_id 2
  @deployment_worker_id 3
  @dev_delete_queue "DevDelete"

  def before_work(pipeline) do
    job = Exq.Support.Job.decode(pipeline.assigns.job_serialized)
    target = String.replace(job.class, "::", ".")
    [mod | _func_or_empty] = Regex.split(~r/\//, target)
    module = String.to_atom("Elixir.#{mod}")

    CogyntLogger.info("#{__MODULE__}", "Queueing Job for #{module}")

    pipeline =
      pipeline
      |> monitor_job
      |> assign(:job, job)
      |> assign(:worker_module, module)

    IO.inspect(Redis.get("dd"),
      label: "******BEFORE WORK JOBS RUNNING"
    )

    pipeline
  end

  def after_processed_work(pipeline) do
    pipeline =
      pipeline
      |> demonitor_job
      |> remove_job_from_backup

    IO.inspect(Redis.get("dd"),
      label: "******AFTER WORK JOBS RUNNING"
    )

    pipeline
  end

  def after_failed_work(pipeline) do
    pipeline
    |> demonitor_job
    |> retry_or_fail_job
    |> remove_job_from_backup
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp retry_or_fail_job(%Pipeline{assigns: assigns} = pipeline) do
    if assigns.job do
      JobQueue.retry_or_fail_job(
        assigns.redis,
        assigns.namespace,
        assigns.job,
        to_string(assigns.error_message)
      )
    end

    pipeline
  end

  defp update_dev_delete_key(value) when is_list(value) do
    case Redis.get("dd") do
      {:ok, nil} ->
        Redis.set("dd", Enum.uniq(value))
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)

      {:ok, values} ->
        Redis.set("dd", Enum.uniq(values ++ value))
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)

      _ ->
        Redis.set("dd", Enum.uniq(value))
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)
    end
  end

  defp update_dev_delete_key(value) do
    case Redis.get("dd") do
      {:ok, nil} ->
        Redis.set("dd", [value])
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)

      {:ok, values} ->
        Redis.set("dd", Enum.uniq(values ++ [value]))
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)

      _ ->
        Redis.set("dd", [value])
        # 1 hour
        Redis.key_pexpire("dd", 3_600_000)
    end
  end

  defp remove_from_dev_delete_key(value) when is_list(value) do
    case Redis.key_exists?("dd") do
      {:ok, true} ->
        case Redis.get("dd") do
          {:ok, nil} ->
            Redis.key_delete("dd")
            Redis.publish_async("dev_delete_subscription", %{action: "stop"})

          {:ok, values} ->
            values = Enum.reject(values, fn x -> x in value end)

            if Enum.empty?(values) do
              Redis.key_delete("dd")
              Redis.publish_async("dev_delete_subscription", %{action: "stop"})
            else
              Redis.set("dd", Enum.uniq(values))
              # 1 hour
              Redis.key_pexpire("dd", 3_600_000)
            end

          _ ->
            Redis.key_delete("dd")
            Redis.publish_async("dev_delete_subscription", %{action: "stop"})
        end

      {:ok, false} ->
        nil
    end
  end

  defp remove_from_dev_delete_key(value) do
    case Redis.key_exists?("dd") do
      {:ok, true} ->
        case Redis.get("dd") do
          {:ok, nil} ->
            Redis.key_delete("dd")
            Redis.publish_async("dev_delete_subscription", %{action: "stop"})

          {:ok, values} ->
            values = Enum.reject(values, fn x -> x == value end)

            if Enum.empty?(values) do
              Redis.key_delete("dd")
              Redis.publish_async("dev_delete_subscription", %{action: "stop"})
            else
              Redis.set("dd", Enum.uniq(values))
              # 1 hour
              Redis.key_pexpire("dd", 3_600_000)
            end

          _ ->
            Redis.key_delete("dd")
            Redis.publish_async("dev_delete_subscription", %{action: "stop"})
        end

      {:ok, false} ->
        nil
    end
  end

  defp remove_job_from_backup(%Pipeline{assigns: assigns} = pipeline) do
    JobQueue.remove_job_from_backup(
      assigns.redis,
      assigns.namespace,
      assigns.host,
      assigns.queue,
      assigns.job_serialized
    )

    pipeline
  end

  defp monitor_job(pipeline) do
    job = Exq.Support.Job.decode(pipeline.assigns.job_serialized)
    args = List.first(job.args)
    worker_module = "Elixir." <> job.class

    cond do
      # Backfill Notifications
      worker_module == to_string(BackfillNotificationsWorker) ->
        case Redis.hash_get("ts", "bn") do
          {:ok, nil} ->
            Redis.hash_set(
              "ts",
              "bn",
              [args]
            )

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "bn",
              Enum.uniq(notification_setting_ids ++ [args])
            )
        end

      # Update Notifications
      worker_module == to_string(UpdateNotificationsWorker) ->
        case Redis.hash_get("ts", "un") do
          {:ok, nil} ->
            Redis.hash_set(
              "ts",
              "un",
              [args]
            )

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "un",
              Enum.uniq(notification_setting_ids ++ [args])
            )
        end

      # Delete Notifications
      worker_module == to_string(DeleteNotificationsWorker) ->
        case Redis.hash_get("ts", "dn") do
          {:ok, nil} ->
            Redis.hash_set(
              "ts",
              "dn",
              [args]
            )

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "dn",
              Enum.uniq(notification_setting_ids ++ [args])
            )
        end

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        update_dev_delete_key(@deployment_worker_id)

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        update_dev_delete_key([@template_solutions_id, @template_solution_events_id])

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        update_dev_delete_key(event_definition_hash_id)

      true ->
        nil
    end

    pipeline
  end

  defp demonitor_job(pipeline) do
    job = Exq.Support.Job.decode(pipeline.assigns.job_serialized)
    args = List.first(job.args)
    worker_module = "Elixir." <> job.class

    cond do
      # Backfill Notifications
      worker_module == to_string(BackfillNotificationsWorker) ->
        case Redis.hash_get("ts", "bn") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "bn",
              List.delete(notification_setting_ids, args)
            )
        end

      # Update Notifications
      worker_module == to_string(UpdateNotificationsWorker) ->
        case Redis.hash_get("ts", "un") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "un",
              List.delete(notification_setting_ids, args)
            )
        end

      # Delete Notifications
      worker_module == to_string(DeleteNotificationsWorker) ->
        case Redis.hash_get("ts", "dn") do
          {:ok, nil} ->
            nil

          {:ok, notification_setting_ids} ->
            Redis.hash_set(
              "ts",
              "dn",
              List.delete(notification_setting_ids, args)
            )
        end

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        remove_from_dev_delete_key(@deployment_worker_id)

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        remove_from_dev_delete_key([
          @template_solutions_id,
          @template_solution_events_id
        ])

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        remove_from_dev_delete_key(event_definition_hash_id)

      true ->
        nil
    end

    pipeline
  end
end
