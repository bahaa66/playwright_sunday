defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteDrilldownDataTask do
  @moduledoc """
  Task module that can bee called to execute the delete_drilldown_data_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Drilldown.DrilldownContext

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
    reset_drilldown()
  end

  defp reset_drilldown(counter \\ 0) do
    if counter >= 6 do
      Redis.key_delete("drilldown_message_info")
      Redis.key_delete("drilldown_event_messages")
      DrilldownContext.hard_delete_template_solutions_data()
      Process.sleep(2000)
      CogyntLogger.info("#{__MODULE__}", "Starting the Drilldown ConsumerGroup")
      ConsumerGroupSupervisor.start_child(:drilldown)
    else
      case finished_processing?() do
        true ->
          Redis.key_delete("drilldown_message_info")
          Redis.key_delete("drilldown_event_messages")
          DrilldownContext.hard_delete_template_solutions_data()
          Process.sleep(2000)
          CogyntLogger.info("#{__MODULE__}", "Starting the Drilldown ConsumerGroup")
          ConsumerGroupSupervisor.start_child(:drilldown)

        false ->
          Process.sleep(10_000)
          reset_drilldown(counter + 1)
      end
    end
  end

  defp finished_processing?() do
    case Redis.key_exists?("drilldown_message_info") do
      {:ok, false} ->
        {:error, :key_does_not_exist}

      {:ok, true} ->
        {:ok, tmc} = Redis.hash_get("drilldown_message_info", "tmc")
        {:ok, tmp} = Redis.hash_get("drilldown_message_info", "tmp")

        {:ok, String.to_integer(tmp) >= String.to_integer(tmc)}
    end
end
