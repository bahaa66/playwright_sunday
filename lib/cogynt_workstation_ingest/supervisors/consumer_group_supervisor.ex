defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for KafkaEx ConsumerGroups. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Servers.Consumers.KafkaConsumer
  alias CogyntWorkstationIngest.Servers.Caches.ConsumerRetryCache
  alias CogyntWorkstationIngest.Servers.ConsumerMonitor

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  # ------------------------------------ #
  # --- Standard Ingestion Consumers --- #
  # ------------------------------------ #
  def start_child(event_definition) do
    # TODO: Temp
    create_kafka_worker(:standard)

    topic = event_definition.topic
    type = event_definition.event_type
    id = event_definition.id

    existing_topics =
      KafkaEx.metadata(worker_name: :standard).topic_metadatas |> Enum.map(& &1.topic)

    if Enum.member?(existing_topics, topic) do
      child_spec = %{
        id: topic,
        start: {
          KafkaEx.ConsumerGroup,
          :start_link,
          consumer_group_options(event_definition)
        },
        restart: :transient,
        shutdown: 5000,
        type: :supervisor
      }

      {:ok, pid} = DynamicSupervisor.start_child(__MODULE__, child_spec)
      ConsumerMonitor.monitor(pid, id, topic, type)
      {:ok, pid}
    else
      ConsumerRetryCache.retry_consumer(event_definition)
      {:error, nil}
    end
  end

  def stop_child(topic) do
    child_pid = Process.whereis(consumer_group_name(topic))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      Process.sleep(1500)
      {:ok, :success}
    else
      {:ok, :success}
    end
  end

  # --------------------------- #
  # --- Drilldown Consumers --- #
  # --------------------------- #

  def start_child() do
    create_kafka_worker(:drilldown)
    create_drilldown_topics()

    child_spec = %{
      id: :DrillDown,
      start: {
        KafkaEx.ConsumerGroup,
        :start_link,
        drilldown_consumer_group_options()
      },
      restart: :transient,
      shutdown: 5000,
      type: :supervisor
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  def stop_child() do
    child_pid = Process.whereis(:DrillDownGroup)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
      Process.sleep(1500)
      {:ok, :success}
    else
      {:ok, :success}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp consumer_group_options(event_definition) do
    topic = event_definition.topic
    id = event_definition.id
    name = "#{topic}-#{id}"

    [
      KafkaConsumer,
      name,
      [topic],
      [
        name: consumer_group_name(topic),
        commit_interval: Config.commit_interval(),
        commit_threshold: Config.commit_threshold(),
        heartbeat_interval: Config.heartbeat_interval(),
        max_restarts: Config.max_restarts(),
        max_seconds: Config.max_seconds(),
        extra_consumer_args: %{event_definition: event_definition}
      ]
    ]
  end

  defp drilldown_consumer_group_options do
    [
      KafkaConsumer,
      "Drilldown-#{UUID.uuid1()}",
      [Config.topic_sols(), Config.topic_sol_events()],
      [
        name: :DrillDownGroup,
        commit_interval: Config.commit_interval(),
        commit_threshold: Config.commit_threshold(),
        heartbeat_interval: Config.heartbeat_interval(),
        max_restarts: Config.max_restarts(),
        max_seconds: Config.max_seconds()
      ]
    ]
  end

  defp create_kafka_worker(worker_name) do
    KafkaEx.create_worker(worker_name,
      consumer_group: "kafka_ex",
      consumer_group_update_interval: 100
    )
  end

  defp create_drilldown_topics do
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
      worker_name: :drilldown,
      timeout: 10_000
    )
  end

  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")
end
