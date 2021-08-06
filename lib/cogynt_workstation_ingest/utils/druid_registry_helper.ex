defmodule CogyntWorkstationIngest.Utils.DruidRegistryHelper do
  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor
  alias CogyntWorkstationIngest.Supervisors.DruidSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinitionDetail
  alias CogyntWorkstationIngest.Config

  @default_dimensions [
    %{
      type: "string",
      name: "id"
    },
    %{
      type: "string",
      name: "published_by"
    },
    %{
      type: "float",
      name: "_confidence"
    },
    %{
      type: "string",
      name: "publishing_template_type_name"
    },
    %{
      type: "string",
      name: "data_type"
    },
    %{
      type: "string",
      name: "$crud"
    },
    %{
      type: "string",
      name: "source"
    },
    %{
      type: "date",
      name: "published_at"
    },
    %{
      type: "string",
      name: "path"
    },
    %{
      type: "string",
      name: "location"
    },
    %{
      type: "string",
      name: "geometry"
    },
    %{
      type: "string",
      name: "coordinates"
    }
  ]

  def start_druid_with_registry_lookup(name, event_definition) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        dimensions =
          EventsContext.get_event_definition_details(event_definition.id)
          |> Enum.reduce(@default_dimensions, fn %EventDefinitionDetail{
                                                   field_name: field_name,
                                                   field_type: field_type
                                                 },
                                                 acc ->
            case field_type do
              "geo" ->
                Enum.uniq(
                  acc ++
                    [
                      %{
                        type: "string",
                        name: field_name
                      }
                    ]
                )

              nil ->
                acc

              _ ->
                Enum.uniq(
                  acc ++
                    [
                      %{
                        type: field_type,
                        name: field_name
                      }
                    ]
                )
            end
          end)

        child_spec = %{
          supervisor_id: event_definition.topic,
          brokers:
            Config.kafka_brokers()
            |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
            |> Enum.join(","),
          dimensions_spec: %{
            dimensions: dimensions
          },
          name: name
        }

        IO.inspect(child_spec, label: "CHILD SPEC ***")

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

  def start_drilldown_druid_with_registry_lookup("template_solutions" = name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        child_spec = %{
          supervisor_id: name,
          schema: :avro,
          schema_registry_url: Config.schema_registry_url(),
          brokers:
            Config.kafka_brokers()
            |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
            |> Enum.join(","),
          dimensions_spec: %{
            dimensions: [
              "id",
              "templateTypeName",
              "templateTypeId",
              "retracted"
            ]
          },
          name: name
        }

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

  def start_drilldown_druid_with_registry_lookup("template_solution_events" = name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        child_spec = %{
          supervisor_id: name,
          schema: :avro,
          schema_registry_url: Config.schema_registry_url(),
          brokers:
            Config.kafka_brokers()
            |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
            |> Enum.join(","),
          dimensions_spec: %{
            dimensions: [
              "id",
              "templateTypeName",
              "templateTypeId",
              "event",
              "aid",
              "assertionName"
            ]
          },
          name: name
        }

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
