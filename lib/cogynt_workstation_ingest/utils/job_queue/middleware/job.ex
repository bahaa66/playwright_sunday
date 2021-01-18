defmodule CogyntWorkstationIngest.Utils.JobQueue.Middleware.Job do
  @behaviour Exq.Middleware.Behaviour
  alias Exq.Redis.JobQueue
  alias Exq.Middleware.Pipeline
  import Pipeline

  alias CogyntWorkstationIngest.Utils.JobQueue.Workers.{
    BackfillNotificationsWorker,
    UpdateNotificationsWorker,
    DeleteNotificationsWorker,
    DeleteEventDefinitionEventsWorker,
    DeleteDeploymentDataWorker,
    DeleteDrilldownDataWorker,
    DeleteEventDefinitionsAndTopicsWorker
  }

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

        Redis.publish_async("notification_settings_subscription", %{
          id: args,
          status: "running"
        })

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

        Redis.publish_async("notification_settings_subscription", %{
          id: args,
          status: "running"
        })

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

        Redis.publish_async("notification_settings_subscription", %{
          id: args,
          status: "running"
        })

      worker_module == to_string(DeleteEventDefinitionEventsWorker) ->
        Redis.hash_set_async("ts", args, %{
          status: "running",
          hard_delete: false
        })

      # TODO: implement handler in pub/sub on OTP
      # Redis.publish_async("event_definitions_subscription", %{
      #   event_definition_ids: [id],
      #   deleting: true
      # })

      worker_module == to_string(DeleteDeploymentDataWorker) ->
        Redis.hash_set("ts", "dptr", "running")

      # TODO: implement handler for this on cogynt-otp
      # Redis.publish_async("deployment_task_status_subscription", %{deleting: true})

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        Redis.hash_set("ts", "dtr", "running")

      # TODO: implement handler for this on cogynt-otp
      # Redis.publish_async("drilldown_task_status_subscription", %{deleting: true})

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_ids" => event_definition_ids
        } = args

        case is_list(event_definition_ids) do
          true ->
            Enum.each(event_definition_ids, fn event_definition_id ->
              Redis.hash_set_async("ts", event_definition_id, %{
                status: "running",
                hard_delete: false
              })
            end)

          # TODO: implement handler for this on cogynt-otp
          # Redis.publish_async("event_definitions_subscription", %{
          #   event_definition_ids: event_definition_ids,
          #   deleting: true
          # })

          false ->
            Redis.hash_set_async("ts", event_definition_ids, %{
              status: "running",
              hard_delete: false
            })

            # TODO: implement handler for this on cogynt-otp
            # Redis.publish_async("event_definitions_subscription", %{
            #   event_definition_ids: [event_definition_ids],
            #   deleting: true
            # })
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

      # TODO: implement pub/sub handler for status: finished on OTP
      # Redis.publish_async("notification_settings_subscription", %{
      #   id: id,
      #   status: "finished"
      # })

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

      # TODO: implement pub/sub handler for status: finished on OTP
      # Redis.publish_async("notification_settings_subscription", %{
      #   id: id,
      #   status: "finished"
      # })

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

      # TODO: implement pub/sub handler for status: finished on OTP
      # Redis.publish_async("notification_settings_subscription", %{
      #   id: id,
      #   status: "finished"
      # })

      worker_module == to_string(DeleteEventDefinitionEventsWorker) ->
        Redis.hash_delete("ts", args)

      # TODO: implement handler in pub/sub on OTP
      # Redis.publish_async("event_definitions_subscription", %{
      #   event_definition_ids: [id],
      #   deleting: false
      # })

      worker_module == to_string(DeleteDeploymentDataWorker) ->
        Redis.hash_delete("ts", "dptr")

      # TODO: implement handler for this on cogynt-otp
      # Redis.publish_async("deployment_task_status_subscription", %{deleting: false})

      worker_module == to_string(DeleteDrilldownDataWorker) ->
        Redis.hash_delete("ts", "dtr")

      # TODO: implement handler for this on cogynt-otp
      # Redis.publish_async("drilldown_task_status_subscription", %{deleting: false})

      worker_module == to_string(DeleteEventDefinitionsAndTopicsWorker) ->
        %{
          "event_definition_ids" => event_definition_ids
        } = args

        case is_list(event_definition_ids) do
          true ->
            Enum.each(event_definition_ids, fn event_definition_id ->
              Redis.hash_delete("ts", event_definition_id)
            end)

          # TODO: implement handler for this on cogynt-otp
          # Redis.publish_async("event_definitions_subscription", %{
          #   event_definition_ids: event_definition_id,
          #   deleting: false
          # })

          false ->
            Redis.hash_delete("ts", event_definition_ids)

            # TODO: implement handler for this on cogynt-otp
            # Redis.publish_async("event_definitions_subscription", %{
            #   event_definition_ids: [event_definition_ids],
            #   deleting: false
            # })
        end

      true ->
        nil
    end

    pipeline
  end
end
