defmodule CogyntWorkstationIngest.Config do
  def env(), do: Application.get_env(:cogynt_workstation_ingest, :environment)
  def deployment_processor_stages(), do: deployment_pipeline()[:processor_stages]
  def deployment_producer_stages(), do: deployment_pipeline()[:producer_stages]

  def event_processor_stages(), do: event_pipeline()[:processor_stages]
  def event_producer_stages(), do: event_pipeline()[:producer_stages]

  def consumer_retry_retry_timer(), do: consumer_retry_worker()[:retry_timer]

  def failed_messages_max_retry(), do: failed_messages()[:max_retry]
  def failed_messages_retry_timer(), do: failed_messages()[:retry_timer]

  def ingest_task_worker_timer(), do: ingest_task_worker()[:timer]

  def kafka_brokers, do: parse_kafka_brokers()
  def kafka_client, do: kafka()[:kafka_client]
  def partition_strategy, do: kafka()[:partition_strategy]
  def template_solutions_topic, do: kafka()[:template_solutions_topic]
  def template_solution_events_topic, do: kafka()[:template_solution_events_topic]
  def partitions, do: kafka()[:partitions]
  def replication_factor, do: kafka()[:replication_factor]
  def replica_assignment, do: kafka()[:replica_assignment]
  def config_entries, do: kafka()[:config_entries]
  def session_timeout, do: kafka()[:session_timeout]
  def deployment_topic(), do: kafka()[:deployment_topic]

  def auth_service_name(), do: rpc()[:cogynt_auth_service_name]
  def auth_service_port(), do: rpc()[:cogynt_auth_service_port]

  def redis_host(), do: redis()[:host]
  def redis_port(), do: redis()[:port]
  def redis_sentinels(), do: redis()[:sentinels]
  def redis_sentinel_group(), do: redis()[:sentinel_group]
  def redis_password(), do: redis()[:password]
  def redis_instance(), do: redis()[:instance]

  def session_key(), do: Application.get_env(:cogynt_workstation_ingest, :session_key)
  def session_domain(), do: Application.get_env(:cogynt_workstation_ingest, :session_domain)
  def signing_salt(), do: Application.get_env(:cogynt_workstation_ingest, :signing_salt)
  def authoring_version(), do: Application.get_env(:cogynt_workstation_ingest, :authoring_version)

  def crud_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:crud]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:crud]
    end
  end

  def linkage_data_type_value() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:link_data_type]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:link_data_type]
    end
  end

  def crud_update_value() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:update]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:update]
    end
  end

  def crud_create_value() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:create]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:create]
    end
  end

  def crud_delete_value() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:delete]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:delete]
    end
  end

  def published_by_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:published_by]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:published_by]
    end
  end

  def published_at_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:published_at]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:published_at]
    end
  end

  def timestamp_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:timestamp]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:timestamp]
    end
  end

  def version_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:version]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:version]
    end
  end

  def id_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:id]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:id]
    end
  end

  def confidence_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:confidence]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:confidence]
    end
  end

  def partial_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:partial]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:partial]
    end
  end

  def entities_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:entities]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:entities]
    end
  end

  def matches_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:matches]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:matches]
    end
  end

  def source_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:source]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:source]
    end
  end

  def data_type_key() do
    if authoring_version() == "1" do
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:data_type]
    else
      Application.get_env(:cogynt_workstation_ingest, :cogynt_keys_v2)[:data_type]
    end
  end

  def startup_delay(), do: startup()[:init_delay]

  def event_index_alias(), do: elasticsearch()[:event_index_alias]

  def http_client(), do: clients()[:http_client]
  def elasticsearch_client(), do: clients()[:elasticsearch_client]
  def rpc_client(), do: clients()[:json_rpc_client]

  def enable_dev_tools?(), do: Application.get_env(:cogynt_workstation_ingest, :enable_dev_tools)

  def postgres_username(), do: postgres()[:username]
  def postgres_password(), do: postgres()[:password]
  def postgres_hostname(), do: postgres()[:hostname]
  def postgres_database(), do: postgres()[:database]

  def schema_registry_url(), do: Application.get_env(:druid, :schema_registry_url)

  def parse_kafka_brokers() do
    String.split(kafka()[:brokers], ",", trim: true)
    |> Enum.reduce([], fn x, acc ->
      broker =
        String.split(x, ":", trim: true)
        |> List.to_tuple()

      port =
        elem(broker, 1)
        |> String.to_integer()

      broker = put_elem(broker, 1, port)

      acc ++ [broker]
    end)
  end

  def parse_kafka_brokers(brokers) do
    String.split(brokers, ",", trim: true)
    |> Enum.reduce([], fn x, acc ->
      broker =
        String.split(x, ":", trim: true)
        |> List.to_tuple()

      port =
        elem(broker, 1)
        |> String.to_integer()

      broker = put_elem(broker, 1, port)

      acc ++ [broker]
    end)
  end

  def libcluster_topologies(), do: Application.get_env(:libcluster, :topologies)

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp postgres(),
    do: Application.get_env(:cogynt_workstation_ingest, CogyntWorkstationIngest.Repo)

  defp kafka, do: Application.get_env(:kafka, :common)

  defp redis, do: Application.get_env(:redis, :common)

  defp deployment_pipeline(),
    do: Application.get_env(:cogynt_workstation_ingest, :deployment_pipeline)

  defp event_pipeline(), do: Application.get_env(:cogynt_workstation_ingest, :event_pipeline)

  defp consumer_retry_worker(),
    do: Application.get_env(:cogynt_workstation_ingest, :consumer_retry_worker)

  defp failed_messages(), do: Application.get_env(:cogynt_workstation_ingest, :failed_messages)

  defp ingest_task_worker(),
    do: Application.get_env(:cogynt_workstation_ingest, :ingest_task_worker)

  defp startup(), do: Application.get_env(:cogynt_workstation_ingest, :startup)

  defp elasticsearch(),
    do:
      Application.get_env(
        :cogynt_workstation_ingest,
        CogyntWorkstationIngest.Elasticsearch.Cluster
      )

  defp clients(), do: Application.get_env(:cogynt_workstation_ingest, :clients)

  defp rpc(), do: Application.get_env(:cogynt_workstation_ingest, :rpc)
end
