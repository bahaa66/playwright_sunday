defmodule CogyntWorkstationIngest.Collections.CollectionsContext do
  @moduledoc """
  The Collections context: public interface for collectionsÏ related functionality.
  """

  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo

  alias Models.Collections.{
    CollectionItem
  }

  # ------------------------------ #
  # --- CollectionItem Methods --- #
  # ------------------------------ #
  @doc """
  Hard deletes collection items and removes their rows from that database.

  ## Examples

      iex> hard_delete_collection_items(%{
        filter: %{item_ids: "59463f0a-9608-11ea-bb37-0242ac130002"}
      })
      {1, nil |[%CollectionItem{}]}

  """
  def hard_delete_collection_items(args) do
    Enum.reduce(args, from(ci in CollectionItem), fn
      {:filter, filter}, q ->
        filter_collection_items(filter, q)
    end)
    |> Repo.delete_all()
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp filter_collection_items(filter, query) do
    Enum.reduce(filter, query, fn
      {:item_ids, item_ids}, q ->
        where(q, [ci], ci.item_id in ^item_ids)

      {:item_type, item_type}, q ->
        where(q, [ci], ci.item_type == ^item_type)
    end)
  end
end
