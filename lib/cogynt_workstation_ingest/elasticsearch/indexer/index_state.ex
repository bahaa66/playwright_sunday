defmodule CogyntWorkstationIngest.Elasticsearch.Indexer.IndexState do
  defstruct [:index, :alias, :error, state: :initializing]

  def transit(%__MODULE__{state: :initializing} = index, :checking_reindex) do
    {:ok, %__MODULE__{index | state: :checking_reindex}}
  end

  def transit(%__MODULE__{state: :initializing} = index, :creating_index) do
    {:ok, %__MODULE__{index | state: :creating_index}}
  end

  def transit(%__MODULE__{state: state} = index, :ready)
      when state in [:checking_reindex, :creating_index] do
    {:ok, %__MODULE__{index | state: :ready}}
  end

  def transit(%__MODULE__{} = index, _) do
    {:error, %__MODULE__{index | state: :creating_index, error: :transition_not_allowed}}
  end

  def transit(%__MODULE__{} = index, :error, opts) do
    {:ok, %__MODULE__{index | state: :error, error: Keyword.get(opts, :error)}}
  end
end
