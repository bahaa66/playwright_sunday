defmodule CogyntWorkstationIngest.Utils.BackfillNotificationsTask do
  @moduledoc """
  Task module that can bee called to execute the backfill_notifications work as a
  async task.
  """
  use Task
  require Logger
  alias CogyntWorkstationIngestWeb.Rpc.CogyntClient
  alias CogyntWorkstationIngest.Notifications.NotificationsContext

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(id) do
    Logger.info("Backfill Notifications Task: Running backfill notifications task for ID: #{id}")

    backfill_notifications(id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp backfill_notifications(id) do
    {:ok, updated_notifications} = NotificationsContext.backfill_notifications(id)
    # Send created_notifications to subscription_queue
    CogyntClient.publish_notifications(updated_notifications)
  end
end
