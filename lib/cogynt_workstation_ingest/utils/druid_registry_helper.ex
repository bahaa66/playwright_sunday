defmodule CogyntWorkstationIngest.Utils.DruidRegistryHelper do
  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor
  alias CogyntWorkstationIngest.Supervisors.DruidSupervisor

  def start_druid_with_registry_lookup(child_spec, name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        case DruidSupervisor.create_druid_supervisor(child_spec) do
          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor, error: #{inspect(error)}"
            )

            {:error, nil}

          _ ->
            {:ok, :success}
        end

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          case SupervisorMonitor.supervisor_status(druid_supervisor_pid) do
            %{state: "SUSPENDED"} ->
              IO.puts("RESUMING DRUID *****")
              SupervisorMonitor.resume_supervisor(druid_supervisor_pid)

            %{state: "RUNNING"} ->
              CogyntLogger.warn(
                "#{__MODULE__}",
                "Druid supervisor: #{name} already running"
              )

            _ ->
              IO.puts("DELETING AND RESETTING DRUID *****")
              SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
          end
        end)

        {:ok, :success}
    end
  end

  def resume_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.resume_supervisor(druid_supervisor_pid)
        end)
    end
  end

  def suspend_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.suspend_supervisor(druid_supervisor_pid)
        end)
    end
  end

  def reset_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
        end)
    end
  end

  def terminate_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.terminate_and_shutdown(druid_supervisor_pid)
        end)
    end
  end
end
