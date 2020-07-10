defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_drilldown_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Servers.Caches.DrilldownCache

  # TODO make sure drilldown is done processing then remove redis keys

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(delete_topics) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_drilldown_data_task with option delete_topics: #{delete_topics}"
    )

    delete_drilldown_data(delete_topics)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_drilldown_data(delete_topics) do
    CogyntLogger.info("#{__MODULE__}", "Stoping the Drilldown ConsumerGroup")
    ConsumerGroupSupervisor.stop_child(:drilldown)

    if delete_topics do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting the Drilldown Topics. #{Config.topic_sols()}, #{Config.topic_sol_events()}"
      )

      delete_topic_result =
        KafkaEx.delete_topics([Config.topic_sols(), Config.topic_sol_events()],
          worker_name: :drilldown
        )

      CogyntLogger.info(
        "#{__MODULE__}",
        "Delete Drilldown Topics result: #{inspect(delete_topic_result, pretty: true)}"
      )
    end

    CogyntLogger.info("#{__MODULE__}", "Resetting Drilldown Cache")
    DrilldownCache.reset_state()
    Process.sleep(2000)
    CogyntLogger.info("#{__MODULE__}", "Starting the Drilldown ConsumerGroup")
    ConsumerGroupSupervisor.start_child(:drilldown)
  end
end
