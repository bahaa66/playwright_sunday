defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDrilldownDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Servers.Druid.{TemplateSolutionEvents, TemplateSolutions}

  def perform(delete_drilldown_topics) do
    # TODO: eventually need to run this against all deployment targets

    # If delete_drilldown_topics is true delete the drilldown topics for the
    # kafka broker assosciated with the deployment_id
    if delete_drilldown_topics do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting the Drilldown Topics. #{Config.template_solutions_topic()}, #{
          Config.template_solution_events_topic()
        }. Brokers: #{inspect(Config.kafka_brokers())}"
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

    CogyntLogger.info("#{__MODULE__}", "Starting resetting of drilldown data")

    # Delete druid data and reset druid supervisors
    TemplateSolutions.delete_data_and_reset_supervisor()
    TemplateSolutionEvents.delete_data_and_reset_supervisor()
  end
end
