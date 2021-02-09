defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteDrilldownDataWorker do
  @moduledoc """
  """
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Drilldown.{DrilldownContextNew, DrilldownSinkConnector}

  def perform(delete_drilldown_topics) do
    # TODO: eventually need to run this against all deployment targets

    # If delete_drilldown_topics is true delete the drilldown topics for the
    # kafka broker assosciated with the deployment_id
    if delete_drilldown_topics do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Deleting the Drilldown Topics. #{Config.template_solutions_topic()}, #{
          Config.template_solution_events_topic()
        }. Brokers: #{Config.kafka_brokers()}"
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
    # TODO: Delete drilldown connectors
    # 1.) Stop Checks
    Redis.publish_async("ingest_channel", %{pause_drilldown_connector_monitor: true})
    # 2.) Delete Connector
    ts_connector =
      DrilldownSinkConnector.fetch_drilldown_connector_cgid(Config.ts_connector_name())

    DrilldownSinkConnector.delete_connector(ts_connector)

    ensure_drilldown_connector_stopped(ts_connector)

    tse_connector =
      DrilldownSinkConnector.fetch_drilldown_connector_cgid(Config.tse_connector_name())

    DrilldownSinkConnector.delete_connector(tse_connector)

    ensure_drilldown_connector_stopped(tse_connector)

    # 3.) Delete Everything
    reset_drilldown_data()
    # 4.) Reenable checks (should recreate connector)
    Redis.publish_async("ingest_channel", %{pause_drilldown_connector_monitor: false})
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #

  defp ensure_drilldown_connector_stopped(connector_name, count \\ 0) do
    if count >= 5 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_drilldown_connector_stopped/2, has ran 5 times and is now returning..."
      )
    else
      case DrilldownSinkConnector.connector_status?(connector_name) do
        {:ok, %{connector: %{state: _state}, tasks: [%{id: _id, state: state}]} = _} ->
          if state == "RUNNING" do
            ensure_drilldown_connector_stopped(connector_name, count + 1)
          end

        {:error, reason} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "ensure_drilldown_connector_stopped/2 failed to check connector status. Reason: #{
              inspect(reason)
            }"
          )
      end
    end
  end

  defp reset_drilldown_data(hashed_brokers \\ nil) do
    if is_nil(hashed_brokers) do
      Redis.hash_delete("dcgid", Config.ts_connector_name())
      Redis.hash_delete("dcgid", Config.tse_connector_name())
    else
      Redis.hash_delete("dcgid", "#{Config.ts_connector_name()}-#{hashed_brokers}")
      Redis.hash_delete("dcgid", "#{Config.tse_connector_name()}-#{hashed_brokers}")
    end

    # TODO: can remove over time
    case Redis.keys_by_pattern("fdm:*") do
      {:ok, []} ->
        nil

      {:ok, failed_message_keys} ->
        Redis.key_delete_pipeline(failed_message_keys)
    end

    # TODO: can remove over time
    case Redis.keys_by_pattern("dmi:*") do
      {:ok, []} ->
        nil

      {:ok, message_info_keys} ->
        Redis.key_delete_pipeline(message_info_keys)
    end

    DrilldownContextNew.hard_delete_template_solutions_data()
  end
end
