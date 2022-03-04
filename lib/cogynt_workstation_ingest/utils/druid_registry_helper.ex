defmodule CogyntWorkstationIngest.Utils.DruidRegistryHelper do
  alias CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor
  alias CogyntWorkstationIngest.Supervisors.DruidSupervisor
  alias CogyntWorkstationIngest.Events.EventsContext
  alias Models.Events.EventDefinitionDetail
  alias CogyntWorkstationIngest.Config

  @timestamp_default "1970-01-01T00:00:00Z"

  def start_druid_with_registry_lookup(event_definition) do
    datasource_name = event_definition.topic

    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        druid_spec = build_druid_ingest_spec(event_definition)

        case DruidSupervisor.start_child(name: datasource_name, druid_spec: druid_spec) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{datasource_name}, error: #{inspect(error)}"
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
              "Druid supervisor: #{datasource_name} already running"
            )

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} has a pending status and is already starting."
            )

          true ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} is being started but has an existing state that is not RUNNING, SUSPENDED, PENDING. Resetting the data and restarting the supervisor."
            )

            SupervisorMonitor.delete_data_and_reset_supervisor(pid)
        end

        {:ok, pid}
    end
  end

  def start_drilldown_druid_with_registry_lookup("template_solutions" = datasource_name) do
    druid_spec =
      build_drilldown_druid_ingestion_spec(
        [
          "id",
          "templateTypeName",
          "templateTypeId",
          "retracted"
        ],
        datasource_name
      )

    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        case DruidSupervisor.start_child(
               name: datasource_name,
               druid_spec: druid_spec,
               force_update: true
             ) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{datasource_name}, error: #{inspect(error)}"
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

  def start_drilldown_druid_with_registry_lookup("template_solution_events" = datasource_name) do
    druid_spec =
      build_drilldown_druid_ingestion_spec(
        [
          "id",
          "templateTypeName",
          "templateTypeId",
          "event",
          "eventId",
          "version",
          "aid",
          "assertionName"
        ],
        datasource_name
      )

    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        case DruidSupervisor.start_child(
               name: datasource_name,
               druid_spec: druid_spec,
               force_update: true
             ) do
          {:error, {:already_started, pid}} ->
            {:ok, pid}

          {:error, error} ->
            CogyntLogger.error(
              "#{__MODULE__}",
              "Failed to start Druid Supervisor: #{datasource_name}, error: #{inspect(error)}"
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

  def update_druid_with_registry_lookup(event_definition) do
    datasource_name = event_definition.topic
    druid_spec = build_druid_ingest_spec(event_definition)

    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "update_druid_with_registry_lookup/2 No supervisor running for: #{datasource_name}. No need to update"
        )

        {:ok, nil}

      pid ->
        SupervisorMonitor.create_or_update_supervisor(pid, druid_spec)
        {:ok, pid}
    end
  end

  def resume_druid_with_registry_lookup(datasource_name) do
    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "resume_druid_with_registry_lookup/1. No PID registred for #{datasource_name}"
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
              "Druid supervisor: #{datasource_name} already running"
            )

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} has a pending status and is already starting."
            )

          true ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} has an unhandled state #{inspect(state)}, Failed to Resume"
            )
        end
    end
  end

  def suspend_druid_with_registry_lookup(datasource_name) do
    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "suspend_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{datasource_name}"
        )

        {:error, :not_found}

      pid ->
        state = SupervisorMonitor.supervisor_status(pid)

        cond do
          SupervisorMonitor.SupervisorStatus.is_suspended?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} already suspended"
            )

          SupervisorMonitor.SupervisorStatus.is_running?(state) ->
            SupervisorMonitor.suspend_supervisor(pid)

          SupervisorMonitor.SupervisorStatus.is_pending?(state) ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} has a pending status you may want to wait and retry."
            )

          true ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Druid supervisor: #{datasource_name} has an unhandled state #{inspect(state)}, Failed to Suspend"
            )
        end
    end
  end

  def drop_and_reset_druid_with_registry_lookup(datasource_name) do
    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "drop_and_reset_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{datasource_name}"
        )

        {:error, :not_found}

      pid ->
        {:ok, SupervisorMonitor.delete_data_and_reset_supervisor(pid)}
    end
  end

  def drop_and_terminate_druid_with_registry_lookup(datasource_name) do
    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "drop_and_terminate_druid_with_registry_lookup/1. No PID registred with DruidRegistry for #{datasource_name}"
        )

        {:error, :not_found}

      pid ->
        {:ok, SupervisorMonitor.delete_data_and_terminate(pid)}
    end
  end

  def check_status_with_registry_lookup(datasource_name) do
    case DruidSupervisor.whereis(datasource_name) do
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "check_status_with_registry_lookup/1. No PID registred with DruidRegistry for #{datasource_name}"
        )

        {:error, :not_found}

      pid ->
        SupervisorMonitor.supervisor_status(pid)
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp build_druid_ingest_spec(event_definition) do
    default_dimensions = default_druid_dimensions()
    default_fields = default_druid_fields()
    # Build the DimensionSpecs and FlattenSpecs based off of the defaults
    # and the EventDefinitionDetails
    {dimensions, fields} =
      EventsContext.get_event_definition_details(event_definition.event_definition_details_id)
      |> Enum.reduce({default_dimensions, default_fields}, fn %EventDefinitionDetail{
                                                                field_type: field_type,
                                                                path: field_path
                                                              },
                                                              {acc_dimensions, acc_fields} ->
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
                      expr: ".#{field_path_to_druid_path(field_path)} | tojson"
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
                      expr: "$.#{field_path_to_druid_path(field_path)}"
                    }
                  ]
              )

            {acc_dimensions, acc_fields}
        end
      end)

    timestamp = build_timestamp_spec(dimensions)

    %{
      supervisor_id: event_definition.topic,
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
    |> IO.inspect(label: "DRUID INGEST SPEC")
  end

  # Druid doesn't like when there are spaces in the 'path' of a field spec
  # so we need to wrap it in [""]
  defp field_path_to_druid_path(field_path) do
    [start | tail] = String.split(field_path, "|")

    Enum.reduce(tail, "[\"#{start}\"]", fn
      i, acc ->
        acc <> ".[\"" <> i <> "\"]"
    end)
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
      timestamp_spec: %{
        column: Config.timestamp_key(),
        format: "auto",
        missingValue: @timestamp_default
      },
      topic: name
    }
  end

  defp build_timestamp_spec(dimensions) do
    case Enum.find(dimensions, fn dimension -> dimension.name == Config.timestamp_key() end) do
      nil ->
        %{
          column: Config.published_at_key(),
          format: "auto",
          missingValue: @timestamp_default
        }

      _ ->
        %{
          column: Config.timestamp_key(),
          format: "auto",
          missingValue: @timestamp_default
        }
    end
  end

  defp default_druid_dimensions() do
    [
      %{
        type: "string",
        name: Config.id_key()
      },
      %{
        type: "string",
        name: Config.published_by_key()
      },
      %{
        type: "float",
        name: Config.confidence_key()
      },
      %{
        type: "string",
        name: Config.crud_key()
      },
      %{
        type: "date",
        name: Config.published_at_key()
      },
      %{
        type: "string",
        name: Config.matches_key()
      },
      %{
        type: "integer",
        name: Config.version_key()
      }
    ]
  end

  defp default_druid_fields() do
    [
      %{
        type: "path",
        name: Config.id_key(),
        expr: "$.#{Config.id_key()}"
      },
      %{
        type: "path",
        name: Config.published_by_key(),
        expr: "$.#{Config.published_by_key()}"
      },
      %{
        type: "path",
        name: Config.confidence_key(),
        expr: "$.#{Config.confidence_key()}"
      },
      %{
        type: "path",
        name: Config.crud_key(),
        expr: "$.#{Config.crud_key()}"
      },
      %{
        type: "path",
        name: Config.published_at_key(),
        expr: "$.#{Config.published_at_key()}"
      },
      %{
        type: "path",
        name: Config.version_key(),
        expr: "$.#{Config.version_key()}"
      },
      %{
        type: "jq",
        name: Config.matches_key(),
        expr: ".[\"#{Config.matches_key()}\"] | tojson"
      }
    ]
  end
end
