defmodule CogyntWorkstationIngest.Utils.DeleteEventIndexDocsTask do
  @moduledoc """
  Task module that can bee called to execute the delete_event_index_docs_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(event_definition_id) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_event_index_docs_task for event_definition_id: #{event_definition_id}"
    )

    delete_event_index_docs(event_definition_id)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_event_index_docs(event_definition_id) do
    {:ok, _} =
      Elasticsearch.delete_by_query(Config.event_index_alias(), %{
        field: "event_definition_id",
        value: event_definition_id
      })
  end
end
