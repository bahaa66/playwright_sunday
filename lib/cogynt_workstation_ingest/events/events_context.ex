defmodule CogyntWorkstationIngest.Events.EventsContext do
  @moduledoc """
  The Events context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias Ecto.Multi

  alias Models.Events.{
    Event,
    EventDefinition,
    EventDetail,
    EventLink
  }

  alias Models.Notifications.Notification
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

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
      [%{}]
      iex> fetch_event_ids(invalid_id)
      nil
  """
  def fetch_event_ids(event_id) do
    query =
      from(d in EventDetail,
        join: e in Event,
        on: e.id == d.event_id,
        where: d.field_value == ^event_id and is_nil(e.deleted_at),
        select: d.event_id
      )

    Repo.all(query)
  end

  @doc """
  Returns a single event_id based on the id event_detail field.
  ## Examples
      iex> fetch_event_id(id)
      UUID
      iex> fetch_event_id(invalid_id)
      nil
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
  Paginates through Events based on the event_definition_id.
  Returns the page_number as a %Scrivener.Page{} object.
  ## Examples
      iex> paginate_events_by_event_definition_id("4123449c-2de0-482f-bea8-5efdb837be08", 1, 10)
      %Scrivener.Page{...}
  """
  def paginate_events_by_event_definition_id(id, page_number, page_size, opts \\ []) do
    preload_details = Keyword.get(opts, :preload_details, true)
    include_deleted = Keyword.get(opts, :include_deleted, false)

    query =
      from(e in Event)
      |> where([e], e.event_definition_id == type(^id, :binary_id))
      |> order_by(desc: :created_at, asc: :id)

    query =
      if preload_details do
        query
        |> preload(:event_details)
      else
        query
      end

    if include_deleted do
      query
    else
      query
      |> where([e], is_nil(e.deleted_at))
    end
    |> Repo.paginate(page: page_number, page_size: page_size)
  end

  @doc """
  Bulk updates many events.
  ## Examples
      iex> update_events(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      {2, [%Event{}, %Event{}]}
  """
  def update_events(args, set: set) do
    Enum.reduce(args, from(n in Event), fn
      {:filter, filter}, q ->
        filter_events(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.update_all(set: set)
  end

  defp filter_events(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [e], e.event_definition_id == ^event_definition_id)

      {:event_ids, event_ids}, q ->
        where(q, [e], e.id in ^event_ids)
    end)
  end

  # -------------------------------------- #
  # --- EventDefinition Schema Methods --- #
  # -------------------------------------- #
  @doc """
  Returns the EventDefinition for id. Raises an error if it does
  not exist
  ## Examples
      iex> get_event_definition!(id)
      {:ok, %EventDefinition{}}
      iex> get_event_definition!(invalid_id)
       ** (Ecto.NoResultsError)
  """
  def get_event_definition!(id) do
    Repo.get!(EventDefinition, id)
    |> Repo.preload(:event_definition_details)
  end

  @doc """
  Returns the EventDefinition for id.
  ## Examples
      iex> get_event_definition(id)
      {:ok, %EventDefinition{}}
      iex> get_event_definition(invalid_id)
       nil
  """
  def get_event_definition(id) do
    Repo.get(EventDefinition, id)
    |> Repo.preload(:event_definition_details)
  end

  @doc """
  Returns a single EventDefinition struct from the query
  ## Examples
      iex> get_event_definition_by(%{id: id})
      {:ok, %EventDefinition{...}}
      iex> get_event_definition_by(%{id: invalid_id})
      nil
  """
  def get_event_definition_by(clauses), do: Repo.get_by(EventDefinition, clauses)

  # ------------------------------- #
  # --- Event Processor Methods --- #
  # ------------------------------- #
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
              where: n.event_id in ^event_ids,
              select:
                {n.event_id, n.user_id, n.tag_id, n.id, n.title, n.notification_setting_id,
                 n.created_at, n.updated_at, n.deleted_at}
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          l_query =
            from(l in EventLink,
              where: l.parent_event_id in ^event_ids or l.child_event_id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
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
              where: n.event_id in ^event_ids,
              select:
                {n.event_id, n.user_id, n.tag_id, n.id, n.title, n.notification_setting_id,
                 n.created_at, n.updated_at, n.deleted_at}
            )

          e_query =
            from(
              e in Event,
              where: e.id in ^event_ids
            )

          l_query =
            from(l in EventLink,
              where: l.parent_event_id in ^event_ids or l.child_event_id in ^event_ids
            )

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
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

  # ----------------------------------- #
  # --- Application Startup Methods --- #
  # ----------------------------------- #
  def initalize_consumers_with_active_event_definitions() do
    Repo.transaction(fn ->
      Repo.stream(
        from(
          ed in EventDefinition,
          where: is_nil(ed.deleted_at),
          where: ed.active == true
        )
      )
      |> Stream.each(fn ed ->
        ed
        |> Repo.preload(:event_definition_details)
        |> event_definition_struct_to_map()
        |> ConsumerGroupSupervisor.start_child()
      end)
      |> Enum.to_list()
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp event_definition_struct_to_map(event_definition) do
    event_definition_details =
      case event_definition do
        %{event_definition_details: %Ecto.Association.NotLoaded{}} ->
          []

        %{event_definition_details: details} ->
          details

        _ ->
          []
      end

    %{
      id: event_definition.id,
      title: event_definition.title,
      topic: event_definition.topic,
      event_type: event_definition.event_type,
      deleted_at: event_definition.deleted_at,
      authoring_event_definition_id: event_definition.authoring_event_definition_id,
      active: event_definition.active,
      created_at: event_definition.created_at,
      updated_at: event_definition.updated_at,
      primary_title_attribute: event_definition.primary_title_attribute,
      fields:
        Enum.reduce(event_definition_details, %{}, fn
          %{field_name: n, field_type: t}, acc ->
            Map.put_new(acc, n, t)

          _, acc ->
            acc
        end)
    }
  end
end
