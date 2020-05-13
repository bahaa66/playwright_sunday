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
  Returns all event_ids that have records matching in the
  Event table with the core_id
  ## Examples
      iex> get_events_by_core_id("4123449c-2de0-482f-bea8-5efdb837be08")
      [%{}]
      iex> get_events_by_core_id("invalid_id")
      nil
  """
  def get_events_by_core_id(core_id) do
    Repo.all(
      from(e in Event,
        where: e.core_id == ^core_id,
        select: e.id
      )
    )
  end

  @doc """
  Will soft delete all events for the event_ids passed in
  ## Examples
      iex> soft_delete_events(["4123449c-2de0-482f-bea8-5efdb837be08"])
      {integer(), nil | [term()]}
      iex> soft_delete_events("invalid_id")
      {integer(), nil | [term()]}
  """
  def soft_delete_events(event_ids) when length(event_ids) > 0 do
    deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

    from(
      e in Event,
      where: e.id in ^event_ids
    )
    |> Repo.update_all(set: [deleted_at: deleted_at])
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

  @doc """
  Query EventDefinitions
  ## Examples
      iex> query_event_definitions(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      [%EventDefinition{}, %EventDefinition{}]
  """
  def query_event_definitions(args) do
    Enum.reduce(args, from(ed in EventDefinition), fn
      {:filter, filter}, q ->
        filter_event_definitions(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.all()
  end

  @doc """
  Query EventDefinitions for core_id
  ## Examples
      iex> get_core_ids_for_event_definition_id("id")
      [core_ids]
  """
  def get_core_ids_for_event_definition_id(event_definition_id) do
    from(ed in EventDefinition)
    |> join(:inner, [ed], e in Event, on: ed.id == e.event_definition_id)
    |> where(
      [ed, e],
      ed.id == ^event_definition_id and is_nil(ed.deleted_at) and is_nil(e.deleted_at)
    )
    |> select([_ed, e], e.core_id)
    |> Repo.all()
  end

  @doc """
  Bulk updates many event_definitions.
  ## Examples
      iex> update_event_definitions(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      {2, [%EventDefinition{}, %EventDefinition{}]}
  """
  def update_event_definitions(args, set: set) do
    Enum.reduce(args, from(ed in EventDefinition), fn
      {:filter, filter}, q ->
        filter_event_definitions(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.update_all(set: set)
  end

  # ------------------------------------ #
  # --- Pipeline Transaction Methods --- #
  # ------------------------------------ #
  @doc """
  Builds and executes a transaction for all of the fields that were
  built throughout the event and link_event pipeline
  ## Examples
      iex> execute_pipeline_transaction(%{
        event_details: event_details,
        event_docs: event_docs,
        risk_history_doc: risk_history_doc,
        notifications: notifications,
        event_links: event_links,
        delete_ids: event_ids,
        delete_docs: doc_ids
      })
      {:ok, %{}}
      iex> execute_pipeline_transaction(%{field: bad_value})
      {:error, reason}
  """
  def execute_pipeline_transaction(%{
        event_details: event_details,
        notifications: notifications,
        delete_ids: event_ids,
        link_events: link_events,
        event: event
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
            case event["id"] do
              nil ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids
                )

              core_id ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids or l.core_id == ^core_id
                )
            end

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
      end

    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
    |> Multi.insert_all(:insert_event_links, EventLink, link_events)
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

  def execute_pipeline_transaction(%{
        event_details: event_details,
        delete_ids: event_ids,
        link_events: link_events,
        event: event
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
            case event["id"] do
              nil ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids
                )

              core_id ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids or l.core_id == ^core_id
                )
            end

          deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

          Multi.new()
          |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_notifications, n_query, set: [deleted_at: deleted_at])
          |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
      end

    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
    |> Multi.insert_all(:insert_event_links, EventLink, link_events)
    |> Repo.transaction()
  end

  def execute_pipeline_transaction(%{
        event_details: event_details,
        notifications: notifications,
        delete_ids: event_ids,
        event: event
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
            case event["id"] do
              nil ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids
                )

              core_id ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids or l.core_id == ^core_id
                )
            end

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

  def execute_pipeline_transaction(%{
        event_details: event_details,
        delete_ids: event_ids,
        event: event
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
            case event["id"] do
              nil ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids
                )

              core_id ->
                from(
                  l in EventLink,
                  where: l.linkage_event_id in ^event_ids or l.core_id == ^core_id
                )
            end

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

  defp filter_events(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [e], e.event_definition_id == ^event_definition_id)

      {:event_definition_ids, event_definition_ids}, q ->
        where(q, [e], e.event_definition_id in ^event_definition_ids)

      {:event_ids, event_ids}, q ->
        where(q, [e], e.id in ^event_ids)
    end)
  end

  defp filter_event_definitions(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [ed], ed.id == ^event_definition_id)

      {:event_definition_ids, event_definition_ids}, q ->
        where(q, [ed], ed.id in ^event_definition_ids)
    end)
  end
end
