defmodule CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor do
  @moduledoc """
  DymanicSupervisor module for KafkaEx ConsumerGroups. Is started under the
  CogyntWorkstationIngest application Supervision tree. Allows application to dynamically
  start and stop children based on event_definition and topics.
  """
  use DynamicSupervisor

  alias CogyntWorkstationIngest.KafkaConsumer

  def start_link(arg) do
    DynamicSupervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """

  """
  def start_child(event_definition) do
    topic = event_definition.topic

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

      DynamicSupervisor.start_child(__MODULE__, child_spec)
    else
      # EventConsumerRetryCache.add_event_consumer_for_retry(event_definition)
      {:ok, nil}
    end
  end

  def start_children(event_definitions) do
    Enum.each(event_definitions, fn event_definition ->
      start_child(event_definition)
    end)
  end

  @doc """

  """
  def stop_child(topic) do
    child_pid = Process.whereis(consumer_group_name(topic))

    if child_pid != nil do
      DynamicSupervisor.terminate_child(__MODULE__, child_pid)
    end
  end

  def stop_children(topics) do
    Enum.each(topics, fn topic ->
      stop_child(topic)
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp consumer_group_options(event_definition) do
    topic = event_definition.topic
    id = event_definition.id
    # name = "#{topic}-#{id}"
    name = "#{topic}-#{Ecto.UUID.generate()}" # for testing purposes only

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

  # ------------- #
  # ---Configs--- #
  # ------------- #
  defp config(), do: Application.get_env(:kafka_ex, :config)
  defp commit_interval(), do: config()[:commit_interval]
  defp commit_threshold(), do: config()[:commit_threshold]
  defp consumer_group_name(topic), do: String.to_atom(topic <> "Group")
end
