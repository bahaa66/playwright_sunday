defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for Kafka ConsumerGroups. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias CogyntWorkstationIngest.Broadway.{
    EventPipeline,
    DeploymentPipeline,
    DrilldownPipeline
  }

  alias Models.Deployments.Deployment

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_child(event_definition) when is_map(event_definition) do
    {:ok, uris} = DeploymentsContext.get_kafka_brokers(event_definition.deployment_id)

    topic = event_definition.topic

    {:ok, metadata} = :brod.get_metadata(uris, :all)

    if Enum.member?(metadata.topic_metadata, topic) do
      consumer_group_id =
        case Redis.hash_get("ecgid", "EventDefinition-#{event_definition.id}") do
          {:ok, nil} ->
            id = "#{UUID.uuid1()}"
            Redis.hash_set("ecgid", "EventDefinition-#{event_definition.id}", id)
            "EventDefinition-#{event_definition.id}" <> "-" <> id

          {:ok, consumer_group_id} ->
            "EventDefinition-#{event_definition.id}" <> "-" <> consumer_group_id
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
              hosts: uris,
              event_definition_id: event_definition.id,
              event_type: event_definition.event_type
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

  def start_child(:deployment) do
    {:ok, metadata} = :brod.get_metadata(Config.kafka_brokers(), :all)

    if Enum.member?(metadata.topic_metadata, "deployment") do
      consumer_group_id =
        case Redis.hash_get("dpcgid", "Deployment") do
          {:ok, nil} ->
            id = "#{UUID.uuid1()}"
            Redis.hash_set("dpcgid", "Deployment", id)
            "Deployment" <> "-" <> id

          {:ok, consumer_group_id} ->
            "Deployment" <> "-" <> consumer_group_id
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

  def start_child(:drilldown, deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        create_drilldown_topics(Config.kafka_brokers())

        consumer_group_id =
          case Redis.hash_get("dcgid", "Drilldown") do
            {:ok, nil} ->
              id = "#{UUID.uuid1()}"
              Redis.hash_set("dcgid", "Drilldown", id)
              "Drilldown" <> "-" <> id

            {:ok, consumer_group_id} ->
              "Drilldown" <> "-" <> consumer_group_id
          end

        child_spec = %{
          id: :DrillDown,
          start: {
            DrilldownPipeline,
            :start_link,
            [
              %{
                group_id: consumer_group_id,
                topics: [Config.topic_sols(), Config.topic_sol_events()],
                hosts: Config.kafka_brokers()
              }
            ]
          },
          restart: :transient,
          shutdown: 5000,
          type: :supervisor
        }

        DynamicSupervisor.start_child(__MODULE__, child_spec)

      %Deployment{id: id} ->
        {:ok, uris} = DeploymentsContext.get_kafka_brokers(id)

        hash_string = Integer.to_string(:erlang.phash2(uris))

        consumer_group_id =
          case Redis.hash_get("dcgid", "Drilldown-#{hash_string}") do
            {:ok, nil} ->
              id = "#{UUID.uuid1()}"
              Redis.hash_set("dcgid", "Drilldown-#{hash_string}", id)
              "Drilldown-#{hash_string}" <> "-" <> id

            {:ok, consumer_group_id} ->
              "Drilldown-#{hash_string}" <> "-" <> consumer_group_id
          end

        create_drilldown_topics(uris)

        child_spec = %{
          id: :DrillDown,
          start: {
            DrilldownPipeline,
            :start_link,
            [
              %{
                group_id: consumer_group_id,
                topics: [Config.topic_sols(), Config.topic_sol_events()],
                hosts: uris
              }
            ]
          },
          restart: :transient,
          shutdown: 5000,
          type: :supervisor
        }

        DynamicSupervisor.start_child(__MODULE__, child_spec)
    end
  end

  def stop_child(event_definition_id) when is_binary(event_definition_id) do
    consumer_group_id = fetch_event_cgid(event_definition_id)

    child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      Process.sleep(1500)
      {:ok, :success}
    else
      {:ok, :success}
    end
  end

  def stop_child(:deployment) do
    consumer_group_id = fetch_deployment_cgid()

    child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      Process.sleep(1500)
      {:ok, :success}
    else
      {:ok, :success}
    end
  end

  def stop_child(:drilldown, deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        consumer_group_id = fetch_drilldown_cgid()

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        if child_pid != nil do
          DynamicSupervisor.terminate_child(__MODULE__, child_pid)
        else
          {:ok, :success}
        end

      %Deployment{id: id} ->
        {:ok, uris} = DeploymentsContext.get_kafka_brokers(id)
        hash_string = Integer.to_string(:erlang.phash2(uris))

        consumer_group_id = fetch_drilldown_cgid(hash_string)

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        if child_pid != nil do
          DynamicSupervisor.terminate_child(__MODULE__, child_pid)
          Process.sleep(1500)
          {:ok, :success}
        else
          {:ok, :success}
        end
    end
  end

  def drilldown_consumer_running?(deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        consumer_group_id = fetch_drilldown_cgid()

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end

      %Deployment{id: id} ->
        {:ok, uris} = DeploymentsContext.get_kafka_brokers(id)
        hash_string = Integer.to_string(:erlang.phash2(uris))

        consumer_group_id = fetch_drilldown_cgid(hash_string)

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end
    end
  end

  def event_consumer_running?(event_definition_id) do
    consumer_group_id = fetch_event_cgid(event_definition_id)
    child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

    case is_nil(child_pid) do
      true ->
        false

      false ->
        true
    end
  end

  def deployment_consumer_running?() do
    consumer_group_id = fetch_deployment_cgid()
    child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

    case is_nil(child_pid) do
      true ->
        false

      false ->
        true
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp fetch_drilldown_cgid(hash_string \\ nil) do
    if is_nil(hash_string) do
      case Redis.hash_get("dcgid", "Drilldown") do
        {:ok, nil} ->
          ""

        {:ok, consumer_group_id} ->
          "Drilldown" <> "-" <> consumer_group_id
      end
    else
      case Redis.hash_get("dcgid", "Drilldown-#{hash_string}") do
        {:ok, nil} ->
          ""

        {:ok, consumer_group_id} ->
          "Drilldown-#{hash_string}" <> "-" <> consumer_group_id
      end
    end
  end

  defp fetch_deployment_cgid() do
    case Redis.hash_get("dpcgid", "Deployment") do
      {:ok, nil} ->
        ""

      {:ok, consumer_group_id} ->
        "Deployment" <> "-" <> consumer_group_id
    end
  end

  defp fetch_event_cgid(event_definition_id) do
    case Redis.hash_get("ecgid", "EventDefinition-#{event_definition_id}") do
      {:ok, nil} ->
        ""

      {:ok, consumer_group_id} ->
        "EventDefinition-#{event_definition_id}" <> "-" <> consumer_group_id
    end
  end

  defp create_drilldown_topics(brokers) do
    :brod.create_topics(
      brokers,
      [
        %{
          topic: Config.topic_sols(),
          num_partitions: Config.partitions(),
          replication_factor: Config.replication(),
          replica_assignment: [],
          config_entries: Config.topic_config()
        },
        %{
          topic: Config.topic_sol_events(),
          num_partitions: Config.partitions(),
          replication_factor: Config.replication(),
          replica_assignment: [],
          config_entries: Config.topic_config()
        }
      ],
      timeout: 10_000
    )
  end
end
