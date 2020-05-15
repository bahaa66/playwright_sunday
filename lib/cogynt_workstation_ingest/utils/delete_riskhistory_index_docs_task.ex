defmodule CogyntWorkstationIngest.Utils.DeleteRiskHistoryIndexDocsTask do
  @moduledoc """
  Task module that can bee called to execute the delete_riskhistory_index_docs_task work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(core_ids) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_riskhistory_index_docs_task for core_ids: #{core_ids}"
    )

    delete_riskhistory_index_docs(core_ids)
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp delete_riskhistory_index_docs(core_ids) do
    Enum.each(core_ids, fn core_id ->
      {:ok, _} =
        Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
          field: "id",
          value: core_id
        })
    end)
  end
end
