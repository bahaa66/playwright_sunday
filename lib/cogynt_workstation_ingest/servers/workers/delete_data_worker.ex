defmodule CogyntWorkstationIngest.Servers.Workers.DeleteDataWorker do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Supervisors.DynamicTaskSupervisor
  alias CogyntWorkstationIngest.Servers.{DeploymentTaskMonitor, EventDefinitionTaskMonitor}

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def upsert_status(event_definition_id, args) do
    GenServer.cast(__MODULE__, {:upsert_status, event_definition_id, args})
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_args) do
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:upsert_status, event_definition_id, args}, state) do
    new_status = Keyword.get(args, :status)
    hard_delete = Keyword.get(args, :hard_delete, false)

    case Redis.hash_get("ts", event_definition_id) do
      {:ok, nil} ->
        Redis.hash_set_async("ts", event_definition_id, %{
          status: new_status,
          hard_delete: hard_delete
        })

        {:noreply, state}

      {:ok, %{status: status, hard_delete: delete}} ->
        cond do
          status == "waiting" and new_status == "ready" ->
            Redis.hash_set_async("ts", event_definition_id, %{
              status: new_status,
              hard_delete: delete
            })

            DynamicTaskSupervisor.start_child(%{
              delete_event_definitions_and_topics: %{
                event_definition_ids: [event_definition_id],
                hard_delete: delete,
                delete_topics: false
              }
            })

            {:noreply, state}

          status == "ready" and new_status == "running" ->
            Redis.hash_set_async("ts", event_definition_id, %{
              status: new_status,
              hard_delete: delete
            })

            {:noreply, state}

          true ->
            {:noreply, state}
        end

      {:error, reason} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to fetch task status from Redis for id: #{event_definition_id}. Reason: #{
            inspect(reason, pretty: true)
          }"
        )

        {:noreply, state}
    end
  end

  @doc false
  def is_event_type_being_deleted?(event_definition_id) do
    deployment_status = DeploymentTaskMonitor.deployment_task_running?()

    event_definition_status =
      EventDefinitionTaskMonitor.event_definition_task_running?(event_definition_id)

    deployment_status or event_definition_status
  end
end
