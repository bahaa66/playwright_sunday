defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for KafkaEx ConsumerGroups. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Broadway.DrilldownPipeline
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

    worker_name = String.to_atom("deployment#{event_definition.deployment_id}")

    # create KafkaEx worker with kafka brokers from deployment_id
    create_kafka_worker(
      uris: uris,
      name: worker_name
    )

    topic = event_definition.topic

    existing_topics =
      KafkaEx.metadata(worker_name: worker_name).topic_metadatas |> Enum.map(& &1.topic)

    if Enum.member?(existing_topics, topic) do
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
          KafkaEx.ConsumerGroup,
          :start_link,
          consumer_group_options(
            name: consumer_group_id,
            topics: [event_definition.topic],
            consumer_group_name: consumer_group_name(event_definition.id),
            extra_consumer_args: %{event_definition: event_definition}
          )
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
    create_kafka_worker(name: :deployment_stream)

    existing_topics =
      KafkaEx.metadata(worker_name: :deployment_stream).topic_metadatas |> Enum.map(& &1.topic)

    if Enum.member?(existing_topics, "deployment") do
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
          KafkaEx.ConsumerGroup,
          :start_link,
          consumer_group_options(
            name: consumer_group_id,
            topics: [Config.deployment_topic()],
            consumer_group_name: consumer_group_name("Deployment")
          )
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
        create_kafka_worker(name: :drilldown)
        create_drilldown_topics(:drilldown)

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
        worker_name = String.to_atom("drilldown" <> hash_string)

        consumer_group_id =
          case Redis.hash_get("dcgid", "Drilldown-#{hash_string}") do
            {:ok, nil} ->
              id = "#{UUID.uuid1()}"
              Redis.hash_set("dcgid", "Drilldown-#{hash_string}", id)
              "Drilldown-#{hash_string}" <> "-" <> id

            {:ok, consumer_group_id} ->
              "Drilldown-#{hash_string}" <> "-" <> consumer_group_id
          end

        create_kafka_worker(uris: uris, name: worker_name)
        create_drilldown_topics(worker_name)

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
    child_pid = Process.whereis(consumer_group_name(event_definition_id))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      Process.sleep(1500)
      {:ok, :success}
    else
      {:ok, :success}
    end
  end

  def stop_child(:deployment) do
    child_pid = Process.whereis(consumer_group_name("Deployment"))

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
        consumer_group_id =
          case Redis.hash_get("dcgid", "Drilldown") do
            {:ok, nil} ->
              id = "#{UUID.uuid1()}"
              Redis.hash_set("dcgid", "Drilldown", id)
              "Drilldown" <> "-" <> id

            {:ok, consumer_group_id} ->
              "Drilldown" <> "-" <> consumer_group_id
          end

        child_pid = Process.whereis(String.to_atom(consumer_group_id <> "Pipeline"))

        if child_pid != nil do
          DynamicSupervisor.terminate_child(__MODULE__, child_pid)
        else
          {:ok, :success}
        end

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
        consumer_group_id =
          case Redis.hash_get("dcgid", "Drilldown") do
            {:ok, nil} ->
              id = "#{UUID.uuid1()}"
              Redis.hash_set("dcgid", "Drilldown", id)
              "Drilldown" <> "-" <> id

            {:ok, consumer_group_id} ->
              "Drilldown" <> "-" <> consumer_group_id
          end

        child_pid = Process.whereis(String.to_atom(consumer_group_id))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end

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

        child_pid = Process.whereis(String.to_atom(consumer_group_id))

        case is_nil(child_pid) do
          true ->
            false

          false ->
            true
        end
    end
  end

  def consumer_running?(name) do
    child_pid = Process.whereis(consumer_group_name(name))

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
  defp consumer_group_options(opts) do
    name = Keyword.get(opts, :name)
    consumer_group_name = Keyword.get(opts, :consumer_group_name)
    topics = Keyword.get(opts, :topics)
    extra_consumer_args = Keyword.get(opts, :extra_consumer_args, %{})

    [
      KafkaConsumer,
      name,
      topics,
      [
        name: consumer_group_name,
        commit_interval: Config.commit_interval(),
        commit_threshold: Config.commit_threshold(),
        heartbeat_interval: Config.heartbeat_interval(),
        max_restarts: Config.max_restarts(),
        max_seconds: Config.max_seconds(),
        extra_consumer_args: extra_consumer_args
      ]
    ]
  end

  defp create_kafka_worker(opts) do
    uris = Keyword.get(opts, :uris, Config.kafka_brokers())
    name = Keyword.get(opts, :name, :standard)

    KafkaEx.create_worker(name,
      uris: uris,
      consumer_group: "kafka_ex",
      consumer_group_update_interval: 100
    )
  end

  defp create_drilldown_topics(worker_name) do
    KafkaEx.create_topics(
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
      worker_name: worker_name,
      timeout: 10_000
    )
  end

  defp consumer_group_name(name),
    do: String.to_atom(name <> "Group")
end
