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

      DynamicSupervisor.start_child(__MODULE__, child_spec)
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

  def start_child(:drilldown, deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        Kafka.Api.Topic.create_topics([
          Config.template_solutions_topic(),
          Config.template_solution_events_topic()
        ])

        cgid = "#{UUID.uuid1()}"

        consumer_group_id =
          case Redis.hash_set_if_not_exists("dcgid", "Drilldown", cgid) do
            {:ok, 0} ->
              {:ok, existing_id} = Redis.hash_get("dcgid", "Drilldown")
              "Drilldown" <> "-" <> existing_id

            {:ok, 1} ->
              "Drilldown" <> "-" <> cgid
          end

        child_spec = %{
          id: :DrillDown,
          start: {
            DrilldownPipeline,
            :start_link,
            [
              %{
                group_id: consumer_group_id,
                topics: [
                  Config.template_solutions_topic(),
                  Config.template_solution_events_topic()
                ],
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
        {:ok, brokers} = DeploymentsContext.get_kafka_brokers(id)

        hash_string = Integer.to_string(:erlang.phash2(brokers))

        cgid = "#{UUID.uuid1()}"

        consumer_group_id =
          case Redis.hash_set_if_not_exists("dcgid", "Drilldown-#{hash_string}", cgid) do
            {:ok, 0} ->
              {:ok, existing_id} = Redis.hash_get("dcgid", "Drilldown-#{hash_string}")
              "Drilldown-#{hash_string}" <> "-" <> existing_id

            {:ok, 1} ->
              "Drilldown-#{hash_string}" <> "-" <> cgid
          end

        Kafka.Api.Topic.create_topics(
          [
            Config.template_solutions_topic(),
            Config.template_solution_events_topic()
          ],
          brokers: brokers
        )

        child_spec = %{
          id: :DrillDown,
          start: {
            DrilldownPipeline,
            :start_link,
            [
              %{
                group_id: consumer_group_id,
                topics: [
                  Config.template_solutions_topic(),
                  Config.template_solution_events_topic()
                ],
                hosts: brokers
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
    pipeline_name = String.to_atom(consumer_group_id <> "Pipeline")

    child_pid = Process.whereis(pipeline_name)

    if child_pid != nil do
      case ensure_consumer_group_state(consumer_group_id, pipeline_name) do
        {:ok, :stop_consumer} ->
          GenServer.stop(child_pid)
          Process.sleep(1500)
          {:ok, :success}

          {:error, :delete_consumer_group_id} ->
            Redis.hash_delete("ecgid", "EventDefinition-#{event_definition_id}")
            GenServer.stop(child_pid)
            Process.sleep(1500)
            {:ok, :success}

          {:error, :delete_consumer_group_id_and_data} ->
            Redis.hash_delete("ecgid", "EventDefinition-#{event_definition_id}")
            # TODO: call job worker for soft deleting event_definition_id data
            GenServer.stop(child_pid)
            Process.sleep(1500)
            {:ok, :success}
      end
    else
      {:ok, :success}
    end
  end

  def stop_child(:deployment) do
    consumer_group_id = fetch_deployment_cgid()
    pipeline_name = :DeploymentPipeline
    child_pid = Process.whereis(pipeline_name)

    if child_pid != nil do
      case ensure_consumer_group_state(consumer_group_id, pipeline_name) do
        {:ok, :stop_consumer} ->
          GenServer.stop(child_pid)
          Process.sleep(1500)
          {:ok, :success}

          {:error, :delete_consumer_group_id} ->
            Redis.hash_delete("dpcgid", "Deployment")
            GenServer.stop(child_pid)
            Process.sleep(1500)
            {:ok, :success}

          {:error, :delete_consumer_group_id_and_data} ->
            Redis.hash_delete("dpcgid", "Deployment")
            # TODO: call job worker for hard deleting deplpoyment data ??
            GenServer.stop(child_pid)
            Process.sleep(1500)
            {:ok, :success}
      end
    else
      {:ok, :success}
    end
  end

  def stop_child(:drilldown, deployment \\ %Deployment{}) do
    case deployment do
      %Deployment{id: nil} ->
        consumer_group_id = fetch_drilldown_cgid()
        pipeline_name = String.to_atom(consumer_group_id <> "Pipeline")

        child_pid = Process.whereis(pipeline_name)

        if child_pid != nil do
          case ensure_consumer_group_state(consumer_group_id, pipeline_name) do
            {:ok, :stop_consumer} ->
              GenServer.stop(child_pid)
              Process.sleep(1500)
              {:ok, :success}

              {:error, :delete_consumer_group_id} ->
                Redis.hash_delete("dcgid", "Drilldown")
                GenServer.stop(child_pid)
                Process.sleep(1500)
                {:ok, :success}

              {:error, :delete_consumer_group_id_and_data} ->
                Redis.hash_delete("dcgid", "Drilldown")
                # TODO: call job worker for soft deleting drilldown data
                GenServer.stop(child_pid)
                Process.sleep(1500)
                {:ok, :success}
          end
        else
          {:ok, :success}
        end

      %Deployment{id: deployment_id} ->
        consumer_group_id = fetch_drilldown_cgid(deployment_id)
        pipeline_name = String.to_atom(consumer_group_id <> "Pipeline")

        child_pid = Process.whereis(pipeline_name)

        if child_pid != nil do
          case ensure_consumer_group_state(consumer_group_id, pipeline_name) do
            {:ok, :stop_consumer} ->
              GenServer.stop(child_pid)
              Process.sleep(1500)
              {:ok, :success}

              {:error, :delete_consumer_group_id} ->
                {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)
                hash_string = Integer.to_string(:erlang.phash2(brokers))
                Redis.hash_delete("dcgid", "Drilldown-#{hash_string}")
                GenServer.stop(child_pid)
                Process.sleep(1500)
                {:ok, :success}

              {:error, :delete_consumer_group_id_and_data} ->
                {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)
                hash_string = Integer.to_string(:erlang.phash2(brokers))
                Redis.hash_delete("dcgid", "Drilldown-#{hash_string}")
                # TODO: call job worker for soft deleting drilldown data
                GenServer.stop(child_pid)
                Process.sleep(1500)
                {:ok, :success}
          end
        else
          {:ok, :success}
        end
    end
  end

  def fetch_drilldown_cgid(deployment_id \\ nil) do
    if is_nil(deployment_id) do
      case Redis.hash_get("dcgid", "Drilldown") do
        {:ok, nil} ->
          ""

        {:ok, consumer_group_id} ->
          "Drilldown" <> "-" <> consumer_group_id
      end
    else
      {:ok, brokers} = DeploymentsContext.get_kafka_brokers(deployment_id)
      hash_string = Integer.to_string(:erlang.phash2(brokers))

      case Redis.hash_get("dcgid", "Drilldown-#{hash_string}") do
        {:ok, nil} ->
          ""

        {:ok, consumer_group_id} ->
          "Drilldown-#{hash_string}" <> "-" <> consumer_group_id
      end
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
  defp ensure_consumer_group_state(consumer_group_id, pipeline_name, count \\ 1) do
    if count >= 10 do
      CogyntLogger.error("#{__MODULE__}", "ensure_consumer_group_state/3 Failed to fetch offsets from Kafka. Stopping Pipeline")
      {:ok, :stop_consumer}
    else
      case Kafka.Api.ConsumerGroup.fetch_committed_offsets(consumer_group_id) do
        {:ok, results} ->
          consumer_group_topic_info = List.first(results)
          topic = consumer_group_topic_info.topic
          number_of_partitions_with_committed_offsets = Enum.count(consumer_group_topic_info.partition_responses)

          if number_of_partitions_with_committed_offsets <= 0 do
            # This means we are trying to stop a pipeline that never was started long enough
            # to have sent committed offsets to its ConsumerGroup. The saftest thing here to
            # do is to remove the ConsumerGroupId from Redis and stop the Pipeline. This way
            # the next time the pipeline is started it will generate a brand new ConsumerGroupId
            # for its Pipeline
            {:error, :delete_consumer_group_id}
          else

            producer_name =
              Broadway.producer_names(pipeline_name)
              |> List.first()

            brod_client_id = Module.concat([producer_name, Client])

            partition_count =
              case Kafka.Api.Topic.fetch_partition_count(brod_client_id, topic) do
              {:ok, partition_count} ->
                partition_count

              {:error, _} ->
                # The standard default for topics is 10.
                # If we fail to fetch the partition count here we dont
                # want to stop the process so we will just assume the default
                # partition count
                10
            end

            if number_of_partitions_with_committed_offsets < partition_count do
              # This is an error state and we must remove the the ConsumerGroupId
              # and any data that may have been ingested for this pipeline
              {:error, :delete_consumer_group_id_and_data}
            end

            {:ok, :stop_consumer}

          end

        {:error, _} ->
          Process.sleep(1000)
          CogyntLogger.warn("#{__MODULE__}", "Retrying ensure_consumer_group_state/3.... Count: #{count}")
          ensure_consumer_group_state(consumer_group_id, pipeline_name, count + 1)
      end

    end
  end
end
