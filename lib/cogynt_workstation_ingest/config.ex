defmodule CogyntWorkstationIngest.Config do
  def drilldown_processor_stages(), do: drilldown_pipeline()[:processor_stages]
  def drilldown_producer_stages(), do: drilldown_pipeline()[:producer_stages]
  def drilldown_enabled(), do: drilldown_pipeline()[:enabled]

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

  def redis_host(), do: redis()[:host]
  def redis_port(), do: redis()[:port]
  def redis_sentinels(), do: redis()[:sentinels]
  def redis_sentinel_group(), do: redis()[:sentinel_group]
  def redis_password(), do: redis()[:password]
  def redis_instance(), do: redis()[:instance]

  def session_key(), do: Application.get_env(:cogynt_workstation_ingest, :session_key)
  def session_domain(), do: Application.get_env(:cogynt_workstation_ingest, :session_domain)
  def signing_salt(), do: Application.get_env(:cogynt_workstation_ingest, :signing_salt)

  def startup_delay(), do: startup()[:init_delay]

  def event_index_alias(), do: elasticsearch()[:event_index_alias]
  def risk_history_index_alias(), do: elasticsearch()[:risk_history_index_alias]
  def elasticsearch_host(), do: elasticsearch()[:host]

  def http_client(), do: clients()[:http_client]
  def elasticsearch_client(), do: clients()[:elasticsearch_client]

  def enable_dev_tools?(), do: Application.get_env(:cogynt_workstation_ingest, :enable_dev_tools)

  def postgres_username(), do: postgres()[:username]

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp postgres(),
    do: Application.get_env(:cogynt_workstation_ingest, CogyntWorkstationIngest.Repo)

  defp kafka, do: Application.get_env(:kafka, :application)

  defp redis, do: Application.get_env(:redis, :application)

  defp drilldown_pipeline(),
    do: Application.get_env(:cogynt_workstation_ingest, :drilldown_pipeline)

  defp deployment_pipeline(),
    do: Application.get_env(:cogynt_workstation_ingest, :deployment_pipeline)

  defp event_pipeline(), do: Application.get_env(:cogynt_workstation_ingest, :event_pipeline)

  defp consumer_retry_worker(),
    do: Application.get_env(:cogynt_workstation_ingest, :consumer_retry_worker)

  defp failed_messages(), do: Application.get_env(:cogynt_workstation_ingest, :failed_messages)

  defp ingest_task_worker(),
    do: Application.get_env(:cogynt_workstation_ingest, :ingest_task_worker)

  defp startup(), do: Application.get_env(:cogynt_workstation_ingest, :startup)

  defp elasticsearch(), do: Application.get_env(:elasticsearch, :application)

  defp clients(), do: Application.get_env(:cogynt_workstation_ingest, :clients)

  defp parse_kafka_brokers() do
    String.split(kafka()[:brokers], ",", trim: true)
    |> Enum.chunk_every(2)
    |> Enum.reduce([], fn x, acc ->
      broker = List.to_tuple(x)

      port =
        elem(broker, 1)
        |> String.to_integer()

      broker = put_elem(broker, 1, port)

      acc ++ [broker]
    end)
  end
end
