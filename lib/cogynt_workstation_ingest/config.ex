defmodule CogyntWorkstationIngest.Config do
  def drilldown_processor_stages(), do: drilldown_pipeline()[:processor_stages]

  def deployment_processor_stages(), do: deployment_pipeline()[:processor_stages]

  def drilldown_time_delay(), do: drilldown_producer()[:time_delay]
  def drilldown_max_retry(), do: drilldown_producer()[:max_retry]
  def drilldown_producer_allowed_messages(), do: drilldown_producer()[:allowed_messages]
  def drilldown_producer_rate_limit_interval(), do: drilldown_producer()[:rate_limit_interval]

  def event_processor_stages(), do: event_pipeline()[:processor_stages]

  def producer_time_delay(), do: producer()[:time_delay]
  def producer_max_retry(), do: producer()[:max_retry]
  def producer_allowed_messages(), do: producer()[:allowed_messages]
  def producer_rate_limit_interval(), do: producer()[:rate_limit_interval]

  def consumer_retry_time_delay(), do: consumer_retry_cache()[:time_delay]
  def consumer_retry_max_retry(), do: consumer_retry_cache()[:max_retry]

  def deployment_consumer_retry_time_delay(), do: deployment_retry_cache()[:time_delay]
  def deployment_consumer_retry_max_retry(), do: deployment_retry_cache()[:max_retry]

  def partitions(), do: kafka()[:topic_partitions]
  def replication(), do: kafka()[:topic_replication]
  def topic_config(), do: kafka()[:topic_config]
  def topic_sols(), do: kafka()[:template_solution_topic]
  def topic_sol_events(), do: kafka()[:template_solution_event_topic]
  def kafka_brokers(), do: kafka()[:brokers]
  def deployment_topic(), do: kafka()[:deployment_topic]

  def session_key(), do: Application.get_env(:cogynt_workstation_ingest, :session_key)
  def session_domain(), do: Application.get_env(:cogynt_workstation_ingest, :session_domain)
  def signing_salt(), do: Application.get_env(:cogynt_workstation_ingest, :signing_salt)

  def startup_delay(), do: startup()[:init_delay]

  def event_index_alias(), do: elasticsearch()[:event_index_alias]
  def risk_history_index_alias(), do: elasticsearch()[:risk_history_index_alias]
  def elasticsearch_host(), do: elasticsearch()[:host]

  def http_client(), do: clients()[:http_client]
  def elasticsearch_client(), do: clients()[:elasticsearch_client]

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp kafka(), do: Application.get_env(:cogynt_workstation_ingest, :kafka)

  defp drilldown_pipeline(),
    do: Application.get_env(:cogynt_workstation_ingest, :drilldown_pipeline)

  defp deployment_pipeline(),
    do: Application.get_env(:cogynt_workstation_ingest, :deployment_pipeline)

  defp drilldown_producer(),
    do: Application.get_env(:cogynt_workstation_ingest, :drilldown_producer)

  defp event_pipeline(), do: Application.get_env(:cogynt_workstation_ingest, :event_pipeline)

  defp producer(), do: Application.get_env(:cogynt_workstation_ingest, :producer)

  defp consumer_retry_cache(),
    do: Application.get_env(:cogynt_workstation_ingest, :consumer_retry_cache)

  defp deployment_retry_cache(),
    do: Application.get_env(:cogynt_workstation_ingest, :deployment_retry_cache)

  defp startup(), do: Application.get_env(:cogynt_workstation_ingest, :startup)

  defp elasticsearch(), do: Application.get_env(:elasticsearch, :config)

  defp clients(), do: Application.get_env(:cogynt_workstation_ingest, :clients)
end
