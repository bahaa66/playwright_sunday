defmodule CogyntWorkstationIngest.Config do
  def event_pipeline_batch_size(),
    do: Application.get_env(:cogynt_workstation_ingest, :event_pipeline_batch_size)

  def deployment_processor_stages(), do: deployment_pipeline()[:processor_stages]
  def deployment_producer_stages(), do: deployment_pipeline()[:producer_stages]

  def event_processor_stages(), do: event_pipeline()[:processor_stages]
  def event_producer_stages(), do: event_pipeline()[:producer_stages]

  def consumer_retry_retry_timer(), do: consumer_retry_worker()[:retry_timer]

  def failed_messages_max_retry(), do: failed_messages()[:max_retry]
  def failed_messages_retry_timer(), do: failed_messages()[:retry_timer]

  def ingest_task_worker_timer(), do: ingest_task_worker()[:timer]

  def kafka_broker_string, do: kafka()[:brokers]
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
  def audit_topic, do: kafka()[:audit_topic]

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

  def crud_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:crud]
  def linkage_data_type_value(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:link_data_type]
  def crud_update_value(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:update]
  def crud_create_value(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:create]
  def crud_delete_value(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:delete]
  def published_by_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:published_by]
  def published_at_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:published_at]
  def timestamp_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:timestamp]
  def version_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:version]
  def id_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:id]
  def confidence_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:confidence]
  def partial_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:partial]
  def entities_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:entities]
  def matches_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:matches]
  def source_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:source]
  def data_type_key(), do: Application.get_env(:cogynt_workstation_ingest, :cogynt_keys)[:data_type]

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

  def drilldown_enabled?(),
    do: Application.get_env(:cogynt_workstation_ingest, :drilldown_enabled)

  def pod_name(), do: Application.get_env(:cogynt_workstation_ingest, :pod_name)

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
