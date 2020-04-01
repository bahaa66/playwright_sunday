defmodule CogyntWorkstationIngest.Events.EventsContext do
  @moduledoc """
  The Events context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias Ecto.Multi

  alias Models.Events.{
    Event,
    EventDetail,
    EventLink
  }

  alias Models.Notifications.Notification

  # ---------------------------- #
  # --- Event Schema Methods --- #
  # ---------------------------- #
  @doc """
  Creates an event.
  ## Examples
      iex> create_event(%{field: value})
      {:ok, %Event{}}
      iex> create_event(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_event(attrs \\ %{}) do
    %Event{}
    |> Event.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Builds a list of event_ids based on the event_id.
  ## Examples
      iex> fetch_event_ids(id)
      {:ok, [%{}]}
      iex> fetch_event_ids(invalid_id)
      {:error, reason}
  """
  def fetch_event_ids(event_id) do
    query =
      from(d in EventDetail,
        join: e in Event,
        on: e.id == d.event_id,
        where: d.field_value == ^event_id and is_nil(e.deleted_at),
        select: d.event_id
      )

    Repo.transaction(fn ->
      Repo.stream(query)
      |> Enum.to_list()
    end)
  end

  @doc """
  Returns a single event_id based on the id event_detail field.
  ## Examples
      iex> fetch_event_id(id)
      {:ok, event_id}
      iex> fetch_event_id(invalid_id)
      {:error, reason}
  """
  def fetch_event_id(id) do
    from(
      e in Event,
      join: ed in EventDetail,
      on: e.id == ed.event_id,
      where: ed.field_name == "id",
      where: ed.field_value == ^id,
      where: is_nil(e.deleted_at),
      limit: 1,
      select: e.id
    )
    |> Repo.one()
  end

  @doc """
  Builds a Multi transactional object based on the args and executes the transaction.
  ## Examples
      iex> execute_event_processor_transaction(%{
        event_details: event_details,
        event_docs: event_docs,
        risk_history_doc: risk_history_doc,
        notifications: notifications,
        delete_ids: event_ids,
        delete_docs: doc_ids
      })
      {:ok, %{}
      iex> execute_event_processor_transaction(%{field: bad_value})
      {:error, reason}
  """
  def execute_event_processor_transaction(%{
        event_details: event_details,
        notifications: nil,
        delete_ids: event_ids
      }) do
    multi =
      case is_nil(event_ids) or Enum.empty?(event_ids) do
        true ->
          Multi.new()

        false ->
          n_query =
            from(n in Notification,
              where: n.event_id in ^event_ids
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query,
            set: [deleted_at: deleted_at],
            returning: [
              :event_id,
              :user_id,
              :tag_id,
              :id,
              :title,
              :notification_setting_id,
              :created_at,
              :updated_at,
              :deleted_at
            ]
          )
      end

    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
    |> Repo.transaction()
  end

  def execute_event_processor_transaction(%{
        event_details: event_details,
        notifications: notifications,
        delete_ids: event_ids
      }) do
    multi =
      case is_nil(event_ids) or Enum.empty?(event_ids) do
        true ->
          Multi.new()

        false ->
          n_query =
            from(n in Notification,
              where: n.event_id in ^event_ids
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query,
            set: [deleted_at: deleted_at],
            returning: [
              :event_id,
              :user_id,
              :tag_id,
              :id,
              :title,
              :notification_setting_id,
              :created_at,
              :updated_at,
              :deleted_at
            ]
          )
      end

    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
    |> Multi.insert_all(:insert_notifications, Notification, notifications,
      returning: [
        :event_id,
        :user_id,
        :tag_id,
        :id,
        :title,
        :notification_setting_id,
        :created_at,
        :updated_at
      ]
    )
    |> Repo.transaction()
  end

  @doc """
  Builds a Multi transactional object based on the args and executes the transaction.
  ## Examples
      iex> execute_link_event_processor_transaction(%{delete_ids: event_ids, event_links: event_links})
      {:ok, %{}
      iex> execute_link_event_processor_transaction(%{field: bad_value})
      {:error, reason}
  """
  def execute_link_event_processor_transaction(%{delete_ids: event_ids, event_links: event_links}) do
    multi =
      case is_nil(event_ids) or Enum.empty?(event_ids) do
        true ->
          Multi.new()

        false ->
          l_query =
            from(
              l in EventLink,
              where: l.linkage_event_id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
      end

    multi
    |> Multi.insert_all(:insert_event_links, EventLink, event_links)
    |> Repo.transaction()
  end
end
