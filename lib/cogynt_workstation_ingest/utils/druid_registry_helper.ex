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
    },
    %{
      type: "integer",
      name: "$version"
    }
  ]

  @default_fields [
    %{
      type: "path",
      name: "id",
      expr: "$.id"
    },
    %{
      type: "path",
      name: "published_by",
      expr: "$.path"
    },
    %{
      type: "path",
      name: "_confidence",
      expr: "$._confidence"
    },
    %{
      type: "path",
      name: "publishing_template_type_name",
      expr: "$.publishing_template_type_name"
    },
    %{
      type: "path",
      name: "data_type",
      expr: "$.data_type"
    },
    %{
      type: "path",
      name: "$crud",
      expr: "$.$crud"
    },
    %{
      type: "path",
      name: "source",
      expr: "$.source"
    },
    %{
      type: "path",
      name: "published_at",
      expr: "$.published_at"
    },
    %{
      type: "path",
      name: "$version",
      expr: "$.$version"
    },
    %{
      type: "jq",
      name: "$matches",
      expr: ".#{@lexions_expression} | tojson"
    }
  ]

  def start_druid_with_registry_lookup(name, event_definition) do
    case DruidSupervisor.whereis(name) do
      nil ->
        druid_spec = build_druid_ingest_spec(name, event_definition)

        case DruidSupervisor.start_child(name: name, druid_spec: druid_spec) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor, error: #{inspect(error)}"
            )

            {:error, nil}

          {:ok, pid} ->
            {:ok, pid}
        end

      pid ->
        state = SupervisorMonitor.supervisor_status(pid)

        cond do
          SupervisorMonitor.SupervisorStatus.is_suspended?(state) ->
            SupervisorMonitor.resume_supervisor(pid)

          SupervisorMonitor.SupervisorStatus.is_running?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} already running"
            )

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} has a pending status and is already starting."
            )

          true ->
            SupervisorMonitor.delete_data_and_reset_supervisor(pid)
        end

        {:ok, pid}
    end
  end

  def start_drilldown_druid_with_registry_lookup("template_solutions" = name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        druid_spec =
          build_drilldown_druid_ingestion_spec(
            [
              "id",
              "templateTypeName",
              "templateTypeId",
              "retracted"
            ],
            name
          )

        case DruidSupervisor.start_child(name: name, druid_spec: druid_spec) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor, error: #{inspect(error)}"
            )

            {:error, nil}

          {:ok, pid} ->
            {:ok, pid}
        end

      pid ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Druid supervisor: #{name} already started by another node"
        )

        {:ok, pid}
    end
  end

  def start_drilldown_druid_with_registry_lookup("template_solution_events" = name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        druid_spec =
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

        case DruidSupervisor.start_child(name: name, druid_spec: druid_spec) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor, error: #{inspect(error)}"
            )

            {:error, nil}

          {:ok, pid} ->
            {:ok, pid}
        end

      pid ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Druid supervisor: #{name} already started by another node"
        )

        {:ok, pid}
    end
  end

  def update_druid_with_registry_lookup(name, event_definition) do
    druid_spec = build_druid_ingest_spec(name, event_definition)

    case DruidSupervisor.whereis(name) do
      nil ->
        case DruidSupervisor.start_child(name: name, druid_spec: druid_spec) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{name}. Error: #{inspect(error)}"
            )

            {:error, nil}

          {:ok, pid} ->
            {:ok, pid}
        end

      pid ->
        SupervisorMonitor.create_or_update_supervisor(pid, druid_spec)
        {:ok, pid}
    end
  end

  def resume_druid_with_registry_lookup(name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "resume_druid_with_registry_lookup/1. No PID registred for #{name}"
        )

        {:error, :not_found}

      pid ->
        state = SupervisorMonitor.supervisor_status(pid)

        cond do
          SupervisorMonitor.SupervisorStatus.is_suspended?(state) ->
            SupervisorMonitor.resume_supervisor(pid)

          SupervisorMonitor.SupervisorStatus.is_running?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} already running"
            )

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} has a pending status and is already starting."
            )

          true ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} has an unhandled state #{state}."
            )
        end
    end
  end

  def suspend_druid_with_registry_lookup(name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "suspend_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{name}"
        )

        {:error, :not_found}

      pid ->
        state = SupervisorMonitor.supervisor_status(pid)

        cond do
          SupervisorMonitor.SupervisorStatus.is_suspended?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} already suspended"
            )

          SupervisorMonitor.SupervisorStatus.is_running?(state) ->
            SupervisorMonitor.suspend_supervisor(pid)

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} has a pending status you may want to wait and retry."
            )

          true ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{name} has an unhandled state #{state}."
            )
        end
    end
  end

  def reset_druid_with_registry_lookup(name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "reset_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{name}"
        )

        {:error, :not_found}

      pid ->
        SupervisorMonitor.delete_data_and_reset_supervisor(pid)
    end
  end

  def terminate_druid_with_registry_lookup(name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "terminate_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{name}"
        )

        {:error, :not_found}

      pid ->
        SupervisorMonitor.terminate_and_shutdown(pid)
    end
  end

  def check_status_with_registry_lookup(name) do
    case DruidSupervisor.whereis(name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "check_status_with_registry_lookup/1. No PID registred with DruidRegistry for #{name}"
        )

        {:error, :not_found}

      pid ->
        Process.send(pid, :get_status, [:noconnect])
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp build_druid_ingest_spec(name, event_definition) do
    # Build the DimensionSpecs and FlattenSpecs based off of the defaults
    # and the EventDefinitionDetails
    {dimensions, fields} =
      EventsContext.get_event_definition_details(event_definition.event_details_id)
      |> Enum.reduce({@default_dimensions, @default_fields}, fn %EventDefinitionDetail{
                                                                  field_type: field_type,
                                                                  path: field_path
                                                                },
                                                                {acc_dimensions, acc_fields} ->
        sigil_field_path = ~s("#{field_path}")

        cond do
          # Any type that is not supported by Native Druid types need to be matched here
          field_type == "geo" or
              String.contains?(field_type, "array") ->
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
                      expr: ".#{Enum.join(String.split(sigil_field_path, "|"), ".")} | tojson"
                    }
                  ]
              )

            {acc_dimensions, acc_fields}

          field_type == "nil" or is_nil(field_type) ->
            {acc_dimensions, acc_fields}

          true ->
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

    # If the _timestamp field existed in the EventDefinitionDetails then use it as the
    # Druid timestamp filter. Otherwise use published_at
    timestamp =
      case Enum.find(dimensions, fn dimension -> dimension.name == "_timestamp" end) do
        nil ->
          %{
            column: "published_at",
            format: "auto",
            missingValue: "1970-01-01T00:00:00Z"
          }

        _ ->
          %{
            column: "_timestamp",
            format: "auto",
            missingValue: "1970-01-01T00:00:00Z"
          }
      end

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
      timestamp_spec: timestamp,
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
