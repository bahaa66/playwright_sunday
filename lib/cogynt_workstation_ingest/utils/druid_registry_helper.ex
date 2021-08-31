defmodule CogyntWorkstationIngest.Utils.DruidRegistryHelper do
  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor
  alias CogyntWorkstationIngest.Supervisors.DruidSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinitionDetail
  alias CogyntWorkstationIngest.Config

  @lexions_expression ~s("$matches")

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
      name: "$matches"
    }
  ]

  @default_fields [
    %{
      type: "root",
      name: "id"
    },
    %{
      type: "root",
      name: "published_by"
    },
    %{
      type: "root",
      name: "_confidence"
    },
    %{
      type: "root",
      name: "publishing_template_type_name"
    },
    %{
      type: "root",
      name: "data_type"
    },
    %{
      type: "root",
      name: "$crud"
    },
    %{
      type: "root",
      name: "source"
    },
    %{
      type: "root",
      name: "published_at"
    },
    %{
      type: "jq",
      name: "$matches",
      expr: ".#{@lexions_expression} | tojson"
    }
  ]

  def start_druid_with_registry_lookup(name, event_definition) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        child_spec = build_druid_ingest_spec(name, event_definition)

        case DruidSupervisor.create_druid_supervisor(child_spec) do
          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{name}. Error: #{inspect(error)}"
            )

            {:error, nil}

          _ ->
            {:ok, :success}
        end

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          case SupervisorMonitor.supervisor_status(druid_supervisor_pid) do
            %{state: "SUSPENDED"} ->
              SupervisorMonitor.resume_supervisor(druid_supervisor_pid)

            %{state: "RUNNING"} ->
              CogyntLogger.warn(
                "#{__MODULE__}",
                "Druid supervisor: #{name} already running"
              )

            _ ->
              SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
          end
        end)

        {:ok, :success}
    end
  end

  def start_drilldown_druid_with_registry_lookup("template_solutions" = name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        child_spec =
          build_drilldown_druid_ingestion_spec(
            [
              "id",
              "templateTypeName",
              "templateTypeId",
              "retracted"
            ],
            name
          )

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
              SupervisorMonitor.resume_supervisor(druid_supervisor_pid)

            %{state: "RUNNING"} ->
              CogyntLogger.warn(
                "#{__MODULE__}",
                "Druid supervisor: #{name} already running"
              )

            _ ->
              SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
          end
        end)

        {:ok, :success}
    end
  end

  def start_drilldown_druid_with_registry_lookup("template_solution_events" = name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        child_spec =
          build_drilldown_druid_ingestion_spec(
            [
              "id",
              "templateTypeName",
              "templateTypeId",
              "event",
              "aid",
              "assertionName"
            ],
            name
          )

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
              SupervisorMonitor.resume_supervisor(druid_supervisor_pid)

            %{state: "RUNNING"} ->
              CogyntLogger.warn(
                "#{__MODULE__}",
                "Druid supervisor: #{name} already running"
              )

            _ ->
              SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
          end
        end)

        {:ok, :success}
    end
  end

  def update_druid_with_registry_lookup(name, event_definition) do
    child_spec = build_druid_ingest_spec(name, event_definition)

    case Registry.lookup(DruidRegistry, name) do
      [] ->
        case DruidSupervisor.create_druid_supervisor(child_spec) do
          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{name}. Error: #{inspect(error)}"
            )

            {:error, nil}

          _ ->
            {:ok, :success}
        end

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.create_or_update_supervisor(druid_supervisor_pid, child_spec)
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
        CogyntLogger.warn("#{__MODULE__}", "No PID registred with DruidRegistry for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.suspend_supervisor(druid_supervisor_pid)
        end)
    end
  end

  def reset_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred with DruidRegistry for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
        end)
    end
  end

  def terminate_druid_with_registry_lookup(name) do
    case Registry.lookup(DruidRegistry, name) do
      [] ->
        CogyntLogger.warn("#{__MODULE__}", "No PID registred with DruidRegistry for #{name}")

      registered_processes ->
        Enum.each(registered_processes, fn {druid_supervisor_pid, _} ->
          SupervisorMonitor.terminate_and_shutdown(druid_supervisor_pid)
        end)
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp build_druid_ingest_spec(name, event_definition) do
    {dimensions, fields} =
      EventsContext.get_event_definition_details(event_definition.id)
      |> Enum.reduce({@default_dimensions, @default_fields}, fn %EventDefinitionDetail{
                                                                  field_type: field_type,
                                                                  path: field_path
                                                                },
                                                                {acc_dimensions, acc_fields} ->
        cond do
          field_type == "geo" or
              field_type == "array" ->
            acc_dimensions =
              Enum.uniq(
                Enum.map(acc_dimensions, fn dimension ->
                  if dimension.name == field_path,
                    do: %{
                      type: "string",
                      name: field_path
                    },
                    else: dimension
                end) ++
                  [
                    %{
                      type: "string",
                      name: field_path
                    }
                  ]
              )

            acc_fields =
              Enum.uniq(
                acc_fields ++
                  [
                    %{
                      type: "jq",
                      name: field_path,
                      expr: ".#{Enum.join(String.split(field_path, "|"), ".")} | tojson"
                    }
                  ]
              )

            {acc_dimensions, acc_fields}

          field_type == "nil" or is_nil(field_type) ->
            {acc_dimensions, acc_fields}

          true ->
            acc_dimensions =
              Enum.map(acc_dimensions, fn dimension ->
                if dimension.name == field_path,
                  do: %{
                    type: field_type,
                    name: field_path
                  },
                  else: dimension
              end)

            acc_dimensions =
              Enum.uniq(
                Enum.map(acc_dimensions, fn dimension ->
                  if dimension.name == field_path,
                    do: %{
                      type: field_type,
                      name: field_path
                    },
                    else: dimension
                end) ++
                  [
                    %{
                      type: field_type,
                      name: field_path
                    }
                  ]
              )

            acc_fields =
              Enum.uniq(
                acc_fields ++
                  [
                    %{
                      type: "path",
                      name: field_path,
                      expr: "$.#{Enum.join(String.split(field_path, "|"), ".")}"
                    }
                  ]
              )

            {acc_dimensions, acc_fields}
        end
      end)

    %{
      supervisor_id: name,
      brokers:
        Config.kafka_brokers()
        |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
        |> Enum.join(","),
      dimensions_spec: %{
        dimensions: dimensions
      },
      flatten_spec: %{
        useFieldDiscovery: true,
        fields: fields
      },
      topic: event_definition.topic
    }
  end

  defp build_drilldown_druid_ingestion_spec(dimensions, name) do
    %{
      supervisor_id: name,
      schema: :avro,
      schema_registry_url: Config.schema_registry_url(),
      brokers:
        Config.kafka_brokers()
        |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
        |> Enum.join(","),
      dimensions_spec: %{
        dimensions: dimensions
      },
      topic: name
    }
  end
end
