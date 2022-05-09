defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for Broadway Pipelines. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.DataSources.DataSourcesContext
  alias CogyntWorkstationIngest.Servers.BroadwayProducerMonitor

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
    {:ok, brokers} = DataSourcesContext.fetch_brokers(event_definition.data_source_id)

    topic = event_definition.topic

    {:ok, existing_topics} = Kafka.Api.Topic.list_topics(brokers)

    if Enum.member?(existing_topics, topic) do
      cgid = "#{UUID.uuid1()}"

      consumer_group_id =
        case Redis.hash_set_if_not_exists("ecgid", "ED-#{event_definition.id}", cgid) do
          {:ok, 0} ->
            {:ok, existing_id} = Redis.hash_get("ecgid", "ED-#{event_definition.id}")
            "ED-#{event_definition.id}" <> "-" <> existing_id

          {:ok, 1} ->
            "ED-#{event_definition.id}" <> "-" <> cgid
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
              event_definition_hash_id: event_definition.id,
              event_type: event_definition.event_type
            }
          ]
        },
        restart: :transient,
        shutdown: 5000,
        type: :supervisor
      }

      case DynamicSupervisor.start_child(__MODULE__, child_spec) do
        {:ok, pid} ->
          BroadwayProducerMonitor.monitor(
            String.to_atom(consumer_group_id <> "Pipeline"),
            event_definition
          )

          {:ok, pid}

        result ->
          result
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

  def stop_child(event_definition) when is_map(event_definition) do
    name = fetch_event_cgid(event_definition.id)

    (name <> "Pipeline")
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

  def fetch_event_cgid(event_definition_hash_id) do
    case Redis.hash_get("ecgid", "ED-#{event_definition_hash_id}") do
      {:ok, nil} ->
        ""

      {:ok, consumer_group_id} ->
        "ED-#{event_definition_hash_id}" <> "-" <> consumer_group_id
    end
  end
end
