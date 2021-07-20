defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for Broadway Pipelines. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Servers.Druid.DynamicSupervisorMonitor

  alias CogyntWorkstationIngest.Broadway.{
    EventPipeline,
    DeploymentPipeline
  }

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_child(event_definition) when is_map(event_definition) do
    {:ok, brokers} = DeploymentsContext.get_kafka_brokers(event_definition.deployment_id)

    topic = event_definition.topic

    {:ok, existing_topics} = Kafka.Api.Topic.list_topics(brokers)

    if Enum.member?(existing_topics, topic) do
      cgid = "#{UUID.uuid1()}"

      consumer_group_id =
        case Redis.hash_set_if_not_exists("ecgid", "EventDefinition-#{event_definition.id}", cgid) do
          {:ok, 0} ->
            {:ok, existing_id} = Redis.hash_get("ecgid", "EventDefinition-#{event_definition.id}")
            "EventDefinition-#{event_definition.id}" <> "-" <> existing_id

          {:ok, 1} ->
            "EventDefinition-#{event_definition.id}" <> "-" <> cgid
        end

      child_spec = %{
        id: topic,
        start: {
          EventPipeline,
          :start_link,
          [
            %{
              group_id: consumer_group_id,
              topics: [topic],
              hosts: brokers,
              event_definition_id: event_definition.id,
              event_type: event_definition.event_type
            }
          ]
        },
        restart: :transient,
        shutdown: 5000,
        type: :supervisor
      }

      # Start Druid Ingestion if EventDefinition event_history flag is set
      if event_definition.event_history == true do
        case start_druid_supervisor(consumer_group_id, topic) do
          {:ok, :success} ->
            DynamicSupervisor.start_child(__MODULE__, child_spec)

          {:error, nil} ->
            {:error, nil}
        end
      else
        DynamicSupervisor.start_child(__MODULE__, child_spec)
      end
    else
      {:error, nil}
    end
  end

  def start_child(:deployment) do
    {:ok, existing_topics} = Kafka.Api.Topic.list_topics()

    if Enum.member?(existing_topics, "deployment") do
      cgid = "#{UUID.uuid1()}"

      consumer_group_id =
        case Redis.hash_set_if_not_exists("dpcgid", "Deployment", cgid) do
          {:ok, 0} ->
            {:ok, existing_id} = Redis.hash_get("dpcgid", "Deployment")
            "Deployment" <> "-" <> existing_id

          {:ok, 1} ->
            "Deployment" <> "-" <> cgid
        end

      child_spec = %{
        id: :Deployment,
        start: {
          DeploymentPipeline,
          :start_link,
          [
            %{
              group_id: consumer_group_id,
              topics: [Config.deployment_topic()],
              hosts: Config.kafka_brokers()
            }
          ]
        },
        restart: :transient,
        shutdown: 5000,
        type: :supervisor
      }

      DynamicSupervisor.start_child(__MODULE__, child_spec)
    else
      {:error, nil}
    end
  end

  def stop_child(event_definition_id) when is_binary(event_definition_id) do
    (fetch_event_cgid(event_definition_id) <> "Pipeline")
    |> String.to_atom()
    |> Process.whereis()
    |> case do
      nil ->
        {:ok, :success}

      child_pid ->
        GenServer.stop(child_pid)
        Process.sleep(1500)
        {:ok, :success}
    end
  end

  def stop_child(:deployment) do
    Process.whereis(:DeploymentPipeline)
    |> case do
      nil ->
        {:ok, :success}

      child_pid ->
        GenServer.stop(child_pid)
        Process.sleep(1500)
        {:ok, :success}
    end
  end

  def fetch_deployment_cgid() do
    case Redis.hash_get("dpcgid", "Deployment") do
      {:ok, nil} ->
        ""

      {:ok, consumer_group_id} ->
        "Deployment" <> "-" <> consumer_group_id
    end
  end

  def fetch_event_cgid(event_definition_id) do
    case Redis.hash_get("ecgid", "EventDefinition-#{event_definition_id}") do
      {:ok, nil} ->
        ""

      {:ok, consumer_group_id} ->
        "EventDefinition-#{event_definition_id}" <> "-" <> consumer_group_id
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp start_druid_supervisor(consumer_group_id, topic) do
    druid_supervisor_name = String.to_atom(consumer_group_id <> "-DruidSupervisor")
    druid_supervisor_pid = Process.whereis(druid_supervisor_name)

    child_spec = %{
      id: topic,
      start: {
        DynamicSupervisorMonitor,
        :start_link,
        [
          %{
            supervisor_id: topic,
            brokers:
              Config.kafka_brokers()
              |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
              |> Enum.join(","),
            dimensions_spec: %{
              dimensions: []
            },
            name: druid_supervisor_name
          }
        ]
      },
      restart: :permanent,
      shutdown: 5000,
      type: :supervisor
    }

    # IO.inspect(druid_supervisor_name, label: "********* NAME")
    # IO.inspect(druid_supervisor_pid, label: "********* PID")

    if is_nil(druid_supervisor_pid) do
      case DynamicSupervisor.start_child(__MODULE__, child_spec) do
        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Failed to start Druid Supervisor, error: #{inspect(error)}"
          )

          {:error, nil}

        _ ->
          {:ok, :success}
      end
    else
      case DynamicSupervisorMonitor.supervisor_status(druid_supervisor_pid) do
        %{"state" => "SUSPENDED"} ->
          IO.puts("RESUMING DRUID *****")
          DynamicSupervisorMonitor.resume_supervisor(druid_supervisor_pid)

        %{"state" => "RUNNING"} ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Druid supervisor: #{druid_supervisor_name} already running"
          )

        _ ->
          IO.puts("DELETING AND RESETTING DRUID *****")
          DynamicSupervisorMonitor.delete_data_and_reset_supervisor(druid_supervisor_pid)
      end

      {:ok, :success}
    end
  end
end
