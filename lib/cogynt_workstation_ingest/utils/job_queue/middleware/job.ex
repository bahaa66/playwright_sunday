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

  @drilldown_worker_id 1
  @deployment_worker_id 2

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

  defp trigger_devdelete_subscription() do
    case Redis.set_length("dd") do
      {:ok, count} when count <= 0 ->
        Redis.publish_async("dev_delete_subscription", %{action: "stop"})

      {:ok, count} when count > 0 ->
        nil

      _ ->
        Redis.publish_async("dev_delete_subscription", %{action: "stop"})
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
        Redis.add_member_to_set("bn", args)
        Redis.key_pexpire("bn", 3_600_000)

      # Update Notifications
      worker_module == to_string(UpdateNotificationsWorker) ->
        Redis.add_member_to_set("un", args)
        Redis.key_pexpire("un", 3_600_000)

      # Delete Notifications
      worker_module == to_string(DeleteNotificationsWorker) ->
        Redis.add_member_to_set("dn", args)
        Redis.key_pexpire("dn", 3_600_000)

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        Redis.add_member_to_set("dd", @deployment_worker_id)
        Redis.key_pexpire("dd", 3_600_000)

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        Redis.add_member_to_set("dd", @drilldown_worker_id)
        Redis.key_pexpire("dd", 3_600_000)

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        Redis.add_member_to_set("dd", event_definition_hash_id)
        Redis.key_pexpire("dd", 3_600_000)

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
        Redis.remove_member_from_set("bn", args)
        Redis.key_pexpire("bn", 3_600_000)

      # Update Notifications
      worker_module == to_string(UpdateNotificationsWorker) ->
        Redis.remove_member_from_set("un", args)
        Redis.key_pexpire("un", 3_600_000)

      # Delete Notifications
      worker_module == to_string(DeleteNotificationsWorker) ->
        Redis.remove_member_from_set("dn", args)
        Redis.key_pexpire("dn", 3_600_000)

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        Redis.remove_member_from_set("dd", @deployment_worker_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_subscription()

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        Redis.remove_member_from_set("dd", @drilldown_worker_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_subscription()

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        Redis.remove_member_from_set("dd", event_definition_hash_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_subscription()

      true ->
        nil
    end

    pipeline
  end
end
