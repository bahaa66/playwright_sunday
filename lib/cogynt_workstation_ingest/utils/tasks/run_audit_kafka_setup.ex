defmodule CogyntWorkstationIngest.Tasks.RunAuditKafkaSetup do
  @moduledoc """
  Task module that runs the Audit Kafka Setup and Creates the Audit Topic
  """
  alias CogyntWorkstationIngest.Config

  use Kafka.Tasks.RunBaseKafkaSetup,
    create_topic: Config.audit_topic(),
    create_topic_retry: 1000,
    partitions: Config.partitions()
end
