defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDrilldownDataWorker do
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Utils.PinotUtils

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

    # Delete table and schema from Pinot
    PinotUtils.delete_table_and_schema(Config.template_solution_events_topic())
    |> case do
      :ok -> nil
      {:error, error} -> CogyntLogger.error("#{__MODULE__}", error)
    end

    if delete_drilldown_topics do
      # Re-create topics for Drilldown
      create_topic_result =
        Kafka.Api.Topic.create_topics([
          Config.template_solutions_topic(),
          Config.template_solution_events_topic()
        ])

      CogyntLogger.info(
        "#{__MODULE__}",
        "Created Drilldown Topics result: #{inspect(create_topic_result, pretty: true)}"
      )
    end

    # Recreate schema and table in Pinot
    PinotUtils.create_schema_and_table(Config.template_solution_events_topic())
    |> case do
      :ok -> nil
      {:error, error} -> CogyntLogger.error("#{__MODULE__}", error)
    end
  end
end
