defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for KafkaEx ConsumerGroups. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor
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

  @doc """
  Will start a KafkaEx ConsumerGroup for the event_definition.topic.
  If the topic does not exist it will retry to start the consumer
  until the topic exists or a maximum retry counter is hit.
  """
  def start_child(event_definition) do
    topic = event_definition.topic
    type = event_definition.event_type
    id = event_definition.id

    existing_topics = KafkaEx.metadata().topic_metadatas |> Enum.map(& &1.topic)

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

  @doc """
  Will start a KafkaEx ConsumerGroup for Drilldown.
  """
  def start_child do
    create_drilldown_topics()

    child_spec = %{
      id: :DrillDown,
      start: {
        KafkaEx.ConsumerGroup,
        :start_link,
        consumer_group_options()
      },
      restart: :permanent,
      shutdown: 5000,
      type: :supervisor
    }

    DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  @doc """
  Will stop the KafkaEx ConsumerGroup for the topic
  """
  def stop_child(topic) do
    child_pid = Process.whereis(consumer_group_name(topic))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    else
      :ok
    end
  end

  @doc """
  Will stop the KafkaEx ConsumerGroup for Drilldown
  """
  def stop_child do
    child_pid = Process.whereis(:DrillDownGroup)

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    else
      :ok
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
        commit_interval: commit_interval(),
        commit_threshold: commit_threshold(),
        extra_consumer_args: %{event_definition: event_definition}
      ]
    ]
  end

  defp consumer_group_options() do
    [
      KafkaConsumer,
      "Drilldown-#{UUID.uuid1()}",
      [topic_sols(), topic_sol_events()],
      [
        name: :DrillDownGroup,
        commit_interval: commit_interval(),
        commit_threshold: commit_threshold()
      ]
    ]
  end

  defp create_drilldown_topics() do
    topic_sols = topic_sols()
    topic_sol_events = topic_sol_events()
    partitions = partitions()
    replication = replication()
    topic_config = topic_config()

    KafkaEx.create_topics(
      [
        %{
          topic: topic_sols,
          num_partitions: partitions,
          replication_factor: replication,
          replica_assignment: [],
          config_entries: topic_config
        },
        %{
          topic: topic_sol_events,
          num_partitions: partitions,
          replication_factor: replication,
          replica_assignment: [],
          config_entries: topic_config
        }
      ],
      timeout: 10_000
    )
  end

  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")

  # ---------------------- #
  # --- configurations --- #
  # ---------------------- #
  defp commit_interval(), do: Application.get_env(:kafka_ex, :commit_interval)
  defp commit_threshold(), do: Application.get_env(:kafka_ex, :commit_threshold)
  defp partitions(), do: Application.get_env(:kafka_ex, :topic_partitions)
  defp replication(), do: Application.get_env(:kafka_ex, :topic_replication)
  defp topic_config(), do: Application.get_env(:kafka_ex, :topic_config)
  defp topic_sols(), do: Application.get_env(:kafka_ex, :template_solution_topic)
  defp topic_sol_events(), do: Application.get_env(:kafka_ex, :template_solution_event_topic)
end
