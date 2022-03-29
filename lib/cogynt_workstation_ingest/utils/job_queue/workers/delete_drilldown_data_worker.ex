defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDrilldownDataWorker do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.DruidRegistryHelper

  def perform(delete_drilldown_topics) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "RUNNING DELETE DRILLDOWN DATA WORKER. delete_drilldown_topics: #{delete_drilldown_topics}"
    )

    # If delete_drilldown_topics is true delete the drilldown topics
    if delete_drilldown_topics do
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

    # Suspend Supervisors
    DruidRegistryHelper.suspend_druid_with_registry_lookup(
      Config.template_solution_events_topic()
    )

    DruidRegistryHelper.suspend_druid_with_registry_lookup(Config.template_solutions_topic())

    # Drop segments 4 datasources and reset supervisors
    drop_and_reset_druid(Config.template_solution_events_topic())
    drop_and_reset_druid(Config.template_solutions_topic())
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp drop_and_reset_druid(datasource_name) do
    case DruidRegistryHelper.drop_and_reset_druid_with_registry_lookup(datasource_name) do
      {:ok, result} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Dropped segments for Druid Datasource: #{datasource_name} with response: #{inspect(result)}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to drop segments for Druid Datasource: #{datasource_name} with Error: #{inspect(error)}"
        )
    end
  end
end
