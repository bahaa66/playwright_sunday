defmodule CogyntWorkstationIngest.Utils.JobQueue.Middleware.Job do
  @behaviour Exq.Middleware.Behaviour
  alias Exq.Redis.JobQueue
  alias Exq.Middleware.Pipeline
  import Pipeline

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    BackfillNotificationsWorker,
    DeleteNotificationsWorker,
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

  alias CogyntWorkstationIngest.Events.EventsContext

  @template_solutions_temp_id 1
  @template_solution_events_temp_id 2

  def before_work(pipeline) do
    job = Exq.Support.Job.decode(pipeline.assigns.job_serialized)
    target = String.replace(job.class, "::", ".")
    [mod | _func_or_empty] = Regex.split(~r/\//, target)
    module = String.to_atom("Elixir.#{mod}")

    CogyntLogger.info("#{__MODULE__}", "Queueing Job for #{module}")

    pipeline
    |> monitor_job
    |> assign(:job, job)
    |> assign(:worker_module, module)
  end

  def after_processed_work(pipeline) do
    pipeline
    |> demonitor_job
    |> remove_job_from_backup
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
        # 7 mins
        Redis.key_pexpire("dd", 420_000)

      {:ok, values} ->
        Redis.set("dd", Enum.uniq(values ++ value))
        # 7 mins
        Redis.key_pexpire("dd", 420_000)

      _ ->
        Redis.set("dd", Enum.uniq(value))
        # 7 mins
        Redis.key_pexpire("dd", 420_000)
    end
  end

  defp update_dev_delete_key(value) do
    case Redis.get("dd") do
      {:ok, nil} ->
        Redis.set("dd", [value])
        # 7 mins
        Redis.key_pexpire("dd", 420_000)

      {:ok, values} ->
        Redis.set("dd", Enum.uniq(values ++ [value]))
        # 7 mins
        Redis.key_pexpire("dd", 420_000)

      _ ->
        Redis.set("dd", [value])
        # 7 mins
        Redis.key_pexpire("dd", 420_000)
    end
  end

  defp remove_from_dev_delete_key(value) when is_list(value) do
    case Redis.key_exists?("dd") do
      {:ok, true} ->
        case Redis.get("dd") do
          {:ok, nil} ->
            Redis.key_delete("dd")

          {:ok, values} ->
            values = Enum.reject(values, fn x -> x in value end)

            if Enum.empty?(values) do
              Redis.key_delete("dd")
            else
              Redis.set("dd", Enum.uniq(values))
              # 7 mins
              Redis.key_pexpire("dd", 420_000)
            end

          _ ->
            Redis.key_delete("dd")
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

          {:ok, values} ->
            values = List.delete(values, value)
            Redis.set("dd", Enum.uniq(values))
            # 7 mins
            Redis.key_pexpire("dd", 420_000)

          _ ->
            Redis.key_delete("dd")
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

  defp fetch_all_event_definition_ids() do
    EventsContext.list_event_definitions()
    |> Enum.group_by(fn ed -> ed.id end)
    |> Map.keys()
  end

  defp monitor_job(pipeline) do
    job = Exq.Support.Job.decode(pipeline.assigns.job_serialized)
    args = List.first(job.args)
    worker_module = "Elixir." <> job.class

    cond do
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

      worker_module == to_string(DeleteDeploymentDataWorker) ->
        ids = fetch_all_event_definition_ids()
        update_dev_delete_key(ids)
        Redis.publish_async("dev_delete_subscription", %{ids: ids, action: "start"})

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        update_dev_delete_key([@template_solutions_temp_id, @template_solution_events_temp_id])

        Redis.publish_async("dev_delete_subscription", %{
          ids: [@template_solutions_temp_id, @template_solution_events_temp_id],
          action: "start"
        })

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_ids" => event_definition_ids
        } = args

        case is_list(event_definition_ids) do
          true ->
            update_dev_delete_key(event_definition_ids)

            Redis.publish_async("dev_delete_subscription", %{
              ids: event_definition_ids,
              action: "start"
            })

          false ->
            update_dev_delete_key([event_definition_ids])

            Redis.publish_async("dev_delete_subscription", %{
              ids: [event_definition_ids],
              action: "start"
            })
        end

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

      worker_module == to_string(DeleteDeploymentDataWorker) ->
        ids = fetch_all_event_definition_ids()
        remove_from_dev_delete_key(ids)
        Redis.publish_async("dev_delete_subscription", %{ids: [ids], action: "stop"})

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        remove_from_dev_delete_key([
          @template_solutions_temp_id,
          @template_solution_events_temp_id
        ])

        Redis.publish_async("dev_delete_subscription", %{
          ids: [@template_solutions_temp_id, @template_solution_events_temp_id],
          action: "stop"
        })

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_ids" => event_definition_ids
        } = args

        remove_from_dev_delete_key(event_definition_ids)

        case is_list(event_definition_ids) do
          true ->
            Redis.publish_async("dev_delete_subscription", %{
              ids: event_definition_ids,
              action: "stop"
            })

          false ->
            Redis.publish_async("dev_delete_subscription", %{
              ids: [event_definition_ids],
              action: "stop"
            })
        end

      true ->
        nil
    end

    pipeline
  end
end
