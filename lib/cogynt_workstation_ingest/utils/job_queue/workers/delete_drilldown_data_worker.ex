defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDrilldownDataWorker do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper

  def perform(delete_drilldown_topics) do
    # If delete_drilldown_topics is true delete the drilldown topics for the
    # kafka broker assosciated with the deployment_id
    if delete_drilldown_topics do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting the Drilldown Topics. #{Config.template_solutions_topic()}, #{Config.template_solution_events_topic()}. Brokers: #{inspect(Config.kafka_brokers())}"
      )

      # Delete topics for worker
      delete_topic_result =
        Kafka.Api.Topic.delete_topics([
          Config.template_solutions_topic(),
          Config.template_solution_events_topic()
        ])

      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleted Drilldown Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    end

    CogyntLogger.info("#{__MODULE__}", "Starting resetting of Drilldown Druid Data")

    # Suspend Supervisors
    DruidRegistryHelper.suspend_druid_with_registry_lookup(
      Config.template_solution_events_topic()
    )

    DruidRegistryHelper.suspend_druid_with_registry_lookup(Config.template_solutions_topic())

    # Drop segments 4 datasources and reset supervisors
    DruidRegistryHelper.drop_and_reset_druid_with_registry_lookup(
      Config.template_solution_events_topic()
    )

    DruidRegistryHelper.drop_and_reset_druid_with_registry_lookup(
      Config.template_solutions_topic()
    )
  end
end
