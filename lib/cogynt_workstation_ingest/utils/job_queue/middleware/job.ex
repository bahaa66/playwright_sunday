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

  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias Models.Notifications.NotificationSetting

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
    |> demonitor_job(true)
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

  defp trigger_devdelete_start() do
    case Redis.set_length("dd") do
      {:ok, count} when count <= 0 ->
        Redis.publish_async("dev_delete_subscription", %{action: "start"})

      {:ok, count} when count > 0 ->
        nil

      _ ->
        Redis.publish_async("dev_delete_subscription", %{action: "start"})
    end
  end

  defp trigger_devdelete_stop() do
    case Redis.set_length("dd") do
      {:ok, count} when count <= 0 ->
        Redis.publish_async("dev_delete_subscription", %{action: "stop"})
        Redis.key_delete("dd")

      {:ok, count} when count > 0 ->
        nil

      _ ->
        Redis.publish_async("dev_delete_subscription", %{action: "stop"})
        Redis.key_delete("dd")
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
        # Failsafe to make sure that the "being_deleted" field is set to "true" when running the
        # DeleteNotificationsTask
        case NotificationsContext.get_notification_setting(args) do
          %NotificationSetting{} = notification_setting ->
            NotificationsContext.update_notification_setting(notification_setting, %{
              being_deleted: true
            })

          _ ->
            # ignore, should not happen
            nil
        end

        Redis.add_member_to_set("dn", args)
        Redis.key_pexpire("dn", 3_600_000)

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        trigger_devdelete_start()
        Redis.add_member_to_set("dd", @deployment_worker_id)
        Redis.key_pexpire("dd", 3_600_000)

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        trigger_devdelete_start()
        Redis.add_member_to_set("dd", @drilldown_worker_id)
        Redis.key_pexpire("dd", 3_600_000)

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        trigger_devdelete_start()
        Redis.add_member_to_set("dd", event_definition_hash_id)
        Redis.key_pexpire("dd", 3_600_000)

      true ->
        nil
    end

    pipeline
  end

  defp demonitor_job(pipeline, failed \\ false) do
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
        # If the DeleteNotification Task failed and DID NOT delete the Notification Setting
        # yet, update the "being_deleted" field to be "false"
        if failed do
          case NotificationsContext.get_notification_setting(args) do
            %NotificationSetting{} = notification_setting ->
              NotificationsContext.update_notification_setting(notification_setting, %{
                being_deleted: false
              })

            _ ->
              # was deleted, do nothing
              nil
          end
        end

        Redis.remove_member_from_set("dn", args)
        Redis.key_pexpire("dn", 3_600_000)

      # Dev Delete
      worker_module == to_string(DeleteDeploymentDataWorker) ->
        Redis.remove_member_from_set("dd", @deployment_worker_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_stop()

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        Redis.remove_member_from_set("dd", @drilldown_worker_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_stop()

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_hash_id" => event_definition_hash_id
        } = args

        Redis.remove_member_from_set("dd", event_definition_hash_id)
        Redis.key_pexpire("dd", 3_600_000)
        trigger_devdelete_stop()

      true ->
        nil
    end

    pipeline
  end
end
