defmodule CogyntWorkstationIngest.Events.EventsContext do
  @moduledoc """
  The Events context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Repo
  alias Models.Enums.ConsumerStatusTypeEnum
  alias CogyntWorkstationIngest.Supervisors.TaskSupervisor
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager

  alias Models.Events.{
    Event,
    EventDefinition,
    EventDetail,
    EventLink,
    EventDefinitionDetail
  }

  alias Models.EventDetailTemplates.{
    EventDetailTemplate,
    EventDetailTemplateGroup,
    EventDetailTemplateGroupItem
  }

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
  Querys Events based on the filter args
  ## Examples
      iex> query_events(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      [%Event{}, %Event{}]
  """
  def query_events(args) do
    query =
      Enum.reduce(args, from(ns in Event), fn
        {:filter, filter}, q ->
          filter_events(filter, q)

        {:select, select}, q ->
          select(q, ^select)
      end)

    Repo.all(query)
  end

  @doc """
  Returns all event_ids that have records that match for the core_id
  and are not deleted
  ## Examples
      iex> get_events_by_core_id(core_id, event_definition_id)
      [%{}]
      iex> get_events_by_core_id("invalid_id")
      nil
  """
  def get_events_by_core_id(core_id, event_definition_id) do
    event_ids =
      Repo.all(
        from(e in Event,
          join: ed in EventDefinition,
          on: ed.id == e.event_definition_id,
          where: e.core_id == ^core_id and ed.id == ^event_definition_id and is_nil(e.deleted_at),
          select: e.id
        )
      )

    if Enum.empty?(event_ids) do
      nil
    else
      event_ids
    end
  end

  @doc """
  Paginates through Events based on the event_definition_id.
  Returns the page_number as a %Scrivener.Page{} object.
  ## Examples
      iex> get_page_of_events(
        %{
          filter: %{
            event_definition_id: "dec1dcda-7f32-11ea-bc55-0242ac130003"
          }
        },
        page_number: 1
      )
      %Scrivener.Page{
        entries: [%Event{}],
        page_number: 1,
        page_size: 500,
        total_entries: 1200,
        total_pages: 3
      }
  """
  def get_page_of_events(args, opts \\ []) do
    preload_details = Keyword.get(opts, :preload_details, true)
    include_deleted = Keyword.get(opts, :include_deleted, false)
    page = Keyword.get(opts, :page_number, 1)
    page_size = Keyword.get(opts, :page_size, 10)

    query =
      Enum.reduce(args, from(e in Event), fn
        {:filter, filter}, q ->
          filter_events(filter, q)
      end)

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
    |> order_by([e], desc: e.created_at, asc: e.id)
    |> Repo.paginate(page: page, page_size: page_size)
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

  @doc """
  Deletes Event and removes their rows from the database.
  ## Examples
      iex> hard_delete_events(%{
        filter: %{ids: "73c3c043-73ff-4d09-b206-029641880cf5"}
      })
      {3, nil}
  """
  def hard_delete_events(args) do
    Enum.reduce(args, from(e in Event), fn
      {:filter, filter}, q ->
        filter_events(filter, q)
    end)
    |> Repo.delete_all(timeout: 60_000)
  end

  # ---------------------------------- #
  # --- EventDetail Schema Methods --- #
  # ---------------------------------- #
  @doc """
  Deletes EventDetails and removes their rows from the database.
  ## Examples
      ex> hard_delete_event_details(%{
        filter: %{event_ids: "73c3c043-73ff-4d09-b206-029641880cf5"}
      })
      {3, nil}
  """
  def hard_delete_event_details(args) do
    Enum.reduce(args, from(e in EventDetail), fn
      {:filter, filter}, q ->
        filter_event_details(filter, q)
    end)
    |> Repo.delete_all(timeout: 60_000)
  end

  # -------------------------------------- #
  # --- EventDefinition Schema Methods --- #
  # -------------------------------------- #
  @doc """
  Lists all the EventDefinitions stored in the database
  ## Examples
      iex> list_event_definitions()
      [%EventDefinition{}, ...]
  """
  def list_event_definitions do
    Repo.all(EventDefinition)
  end

  @doc """
  Creates an EventDefinition.
  ## Examples
      iex> create_event_definition(%{field: value})
      {:ok, %EventDefinition{}}
      iex> create_event_definition(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_event_definition(attrs \\ %{}) do
    %EventDefinition{}
    |> EventDefinition.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates an EventDefinition.
  ## Examples
      iex> update_event_definition(event_definition, %{field: new_value})
      {:ok, %EventDefinition{}}
      iex> update_event_definition(event_definition, %{field: bad_value})
      {:error, ...}
  """
  def update_event_definition(%EventDefinition{} = event_definition, attrs) do
    event_definition
    |> EventDefinition.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Will create the EventDefinition if no record is found for the event_definition_id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_event_definition(%{field: value})
      {:ok, %EventDefinition{}}
      iex> upsert_event_definition(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def upsert_event_definition(attrs \\ %{}) do
    case get_event_definition_by(%{
           authoring_event_definition_id: attrs.authoring_event_definition_id,
           deployment_id: attrs.deployment_id
         }) do
      nil ->
        result =
          Map.put(attrs, :id, Ecto.UUID.generate())
          |> create_event_definition()

        case result do
          {:ok, %EventDefinition{id: id} = event_definition} ->
            if Map.has_key?(attrs, :fields) do
              create_event_definition_fields(id, attrs.fields)
            end

            {:ok, %EventDefinition{} = event_definition}

          _ ->
            result
        end

      %EventDefinition{} = event_definition ->
        result = update_event_definition(event_definition, attrs)

        case result do
          {:ok, %EventDefinition{id: id} = event_definition} ->
            # Delete all EventDefinitionDetails for id
            hard_delete_event_definition_details(id)
            # Create new EventDefinitionDetails for id
            if Map.has_key?(attrs, :fields) do
              create_event_definition_fields(id, attrs.fields)
            end

            {:ok, %EventDefinition{} = event_definition}

          _ ->
            result
        end
    end
  end

  @doc """
  Returns the EventDefinition for id. Raises an error if it does
  not exist
  ## Examples
      iex> get_event_definition!(id)
      %EventDefinition{}
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
      %EventDefinition{}
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
      %EventDefinition{...}
      iex> get_event_definition_by(%{id: invalid_id})
      nil
  """
  def get_event_definition_by(clauses),
    do: Repo.get_by(from(e in EventDefinition, where: is_nil(e.deleted_at)), clauses)

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
  def query_event_definitions(args, opts \\ []) do
    preload_details = Keyword.get(opts, :preload_details, false)

    query =
      Enum.reduce(args, from(ed in EventDefinition), fn
        {:filter, filter}, q ->
          filter_event_definitions(filter, q)

        {:select, select}, q ->
          select(q, ^select)
      end)

    query =
      if preload_details do
        query
        |> preload(:event_details)
      else
        query
      end

    Repo.all(query)
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
      ed.id == ^event_definition_id and is_nil(ed.deleted_at) and is_nil(e.deleted_at) and
        is_nil(e.core_id) == false
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

  @doc """
  Hard deletes an event definition by removing it's from the database.
  ## Examples
      iex> hard_delete_event_definition(event_definition)
      {:ok, %EventDefinition{}}
  """
  def hard_delete_event_definition(%EventDefinition{} = event_definition) do
    Repo.delete(event_definition)
  end

  @doc """
  Removes all the records in the EventDefinitions table.
  It returns a tuple containing the number of entries
  and any returned result as second element. The second
  element is nil by default unless a select is supplied
  in the delete query
    ## Examples
      iex> hard_delete_event_definitions()
      {10, nil}
  """
  def hard_delete_event_definitions() do
    Repo.delete_all(EventDefinition)
  end

  @doc """
  Converts an EventDefinition struct into a dropping the metadata and timestamp related fields
  """
  def remove_event_definition_virtual_fields(%EventDefinition{} = event_definition),
    do: Map.take(event_definition, EventDefinition.__schema__(:fields))

  def remove_event_definition_virtual_fields([]), do: []

  def remove_event_definition_virtual_fields([%EventDefinition{} = event_definition | tail]) do
    [
      Map.take(event_definition, EventDefinition.__schema__(:fields))
      | remove_event_definition_virtual_fields(tail)
    ]
  end

  # ------------------------------------------ #
  # --- EventDetailTemplate Schema Methods --- #
  # ------------------------------------------ #
  @doc """
  Updates the deleted_at values for all EventDetailTemplate
  data associated with the EventDefinition
  """
  def delete_event_definition_event_detail_templates_data(%EventDefinition{} = event_definition) do
    now = DateTime.truncate(DateTime.utc_now(), :second)

    {_count, event_detail_templates} =
      delete_event_definition_event_detail_templates(event_definition, now)

    {_count, event_detail_templates_groups} =
      delete_event_definition_event_detail_template_groups(event_detail_templates)

    delete_event_definition_event_detail_template_group_items(event_detail_templates_groups)
  end

  @doc """
  Given an %EventDefinition{} struct and a deleted_at timestamp it will update all
  %EventDetailTemplate{} for the event_definition_id to be deleted
    ## Examples
      iex> delete_event_definition_event_detail_templates(%{id: event_definition_id}, deleted_at)
      {count, [%EventDetailTemplate{...}]}
      iex> delete_event_definition_event_detail_templates(%{id: invalid_id}, deleted_at)
      nil
  """
  def delete_event_definition_event_detail_templates(%{id: definition_id}, deleted_at) do
    queryable =
      from(et in EventDetailTemplate)
      |> where([et], et.event_definition_id == ^definition_id)
      |> where([et], is_nil(et.deleted_at))
      |> select([et], et)

    Repo.update_all(queryable, set: [deleted_at: deleted_at])
  end

  @doc """
  Given a list of %EventDetailTemplates{} it will update all
  %EventDetailTemplateGroup{} be deleted
    ## Examples
      iex> delete_event_definition_event_detail_template_groups([%EventDetailTemplate{}])
      {count, [%EventDetailTemplateGroup{...}]}
  """
  def delete_event_definition_event_detail_template_groups(event_detail_templates)
      when is_list(event_detail_templates) do
    template_ids = Enum.map(event_detail_templates, fn t -> t.id end)
    deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

    queryable =
      from(g in EventDetailTemplateGroup)
      |> where([g], g.event_detail_template_id in ^template_ids)
      |> where([g], is_nil(g.deleted_at))
      |> select([g], g)

    Repo.update_all(queryable, set: [deleted_at: deleted_at])
  end

  @doc """
  Given a list of %EventDetailTemplates{} it will update all
  %EventDetailTemplateGroup{} be deleted
    ## Examples
      iex> delete_event_definition_event_detail_template_group_items([%EventDetailTemplateGroup{}])
      {count, [%EventDetailTemplateGroupItem{...}]}
  """
  def delete_event_definition_event_detail_template_group_items(event_detail_template_groups)
      when is_list(event_detail_template_groups) do
    group_ids = Enum.map(event_detail_template_groups, fn g -> g.id end)
    deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

    queryable =
      from(i in EventDetailTemplateGroupItem)
      |> where([i], i.event_detail_template_group_id in ^group_ids)
      |> where([i], is_nil(i.deleted_at))
      |> select([i], i)

    Repo.update_all(queryable, set: [deleted_at: deleted_at])
  end

  # -------------------------------------------- #
  # --- EventDefinitionDetail Schema Methods --- #
  # -------------------------------------------- #
  @doc """
  Creates an EventDefinitionDetail.
  ## Examples
      iex> create_event_definition_detail(%{field: value})
      {:ok, %EventDefinitionDetail{}}
      iex> create_event_definition_detail(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def create_event_definition_detail(attrs \\ %{}) do
    %EventDefinitionDetail{}
    |> EventDefinitionDetail.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Removes all the records in the EventDefinitionDetails table.
  It returns a tuple containing the number of entries
  and any returned result as second element. The second
  element is nil by default unless a select is supplied
  in the delete query
    ## Examples
      iex> hard_delete_event_definitions()
      {10, nil}
  """
  def hard_delete_event_definition_details(id) do
    from(details in EventDefinitionDetail, where: details.event_definition_id == ^id)
    |> Repo.delete_all()
  end

  def hard_delete_event_definition_details() do
    Repo.delete_all(EventDefinitionDetail)
  end

  # -------------------------------- #
  # --- EventLink Schema Methods --- #
  # -------------------------------- #

  def update_event_links(args, set: set) do
    query = from(e in EventLink)

    Enum.reduce(args, query, fn
      {:filter, filter}, q ->
        filter_event_links(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.update_all(set: set)
  end

  # ------------------------------------ #
  # --- Pipeline Transaction Methods --- #
  # ------------------------------------ #
  def insert_all_event_details_multi(multi \\ Multi.new(), event_details) do
    multi
    |> Multi.insert_all(:insert_event_details, EventDetail, event_details)
  end

  def insert_all_event_links_multi(multi \\ Multi.new(), link_events) do
    multi
    |> Multi.insert_all(:insert_event_links, EventLink, link_events)
  end

  def update_all_events_multi(multi \\ Multi.new(), delete_event_ids) do
    case is_nil(delete_event_ids) or Enum.empty?(delete_event_ids) do
      true ->
        multi

      false ->
        deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

        e_query =
          from(
            e in Event,
            where: e.id in ^delete_event_ids
          )

        multi
        |> Multi.update_all(:update_events, e_query, set: [deleted_at: deleted_at])
    end
  end

  def update_all_event_links_multi(multi \\ Multi.new(), delete_event_ids) do
    case is_nil(delete_event_ids) or Enum.empty?(delete_event_ids) do
      true ->
        multi

      false ->
        deleted_at = DateTime.truncate(DateTime.utc_now(), :second)

        l_query =
          from(
            l in EventLink,
            where: l.linkage_event_id in ^delete_event_ids
          )

        multi
        |> Multi.update_all(:update_event_links, l_query, set: [deleted_at: deleted_at])
    end
  end

  def run_multi_transaction(multi) do
    Repo.transaction(multi)
  end

  # ----------------------------------- #
  # --- Application Startup Methods --- #
  # ----------------------------------- #
  def initalize_consumer_states() do
    # Fetch all active EventDefinitions and start their consumers
    event_definitions =
      query_event_definitions(
        %{
          filter: %{
            active: true,
            deleted_at: nil
          }
        },
        preload_detail: true
      )

    Enum.each(event_definitions, fn event_definition ->
      ConsumerStateManager.manage_request(%{
        start_consumer: remove_event_definition_virtual_fields(event_definition)
      })
    end)

    # Fetch all EventDefinitions and check if they were in the middle of
    # any tasks when application was restarted. If so trigger the tasks
    # that were running
    event_definitions =
      query_event_definitions(
        %{
          filter: %{
            deleted_at: nil
          }
        },
        preload_detail: true
      )

    Enum.each(event_definitions, fn event_definition ->
      {:ok, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

      cond do
        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:backfill_notification_task_running] ->
          Enum.each(consumer_state.backfill_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing backfill notifications task: #{inspect(notification_setting_id)}"
            )

            TaskSupervisor.start_child(%{backfill_notifications: notification_setting_id})
          end)

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:update_notification_task_running] ->
          Enum.each(consumer_state.update_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing update notifications task: #{inspect(notification_setting_id)}"
            )

            TaskSupervisor.start_child(%{update_notifications: notification_setting_id})
          end)

        consumer_state.status ==
            ConsumerStatusTypeEnum.status()[:delete_notification_task_running] ->
          Enum.each(consumer_state.delete_notifications, fn notification_setting_id ->
            CogyntLogger.info(
              "#{__MODULE__}",
              "Initalizing delete notifications task: #{inspect(notification_setting_id)}"
            )

            TaskSupervisor.start_child(%{delete_notification_setting: notification_setting_id})
          end)

        true ->
          # No tasks to trigger
          nil
      end
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp create_event_definition_fields(id, fields) do
    Enum.each(fields, fn {key, val} ->
      case is_atom(key) do
        true ->
          create_event_definition_detail(%{
            event_definition_id: id,
            field_name: Atom.to_string(key),
            field_type: val.dataType
          })

        false ->
          create_event_definition_detail(%{
            event_definition_id: id,
            field_name: key,
            field_type: val["dataType"]
          })
      end
    end)
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

  defp filter_event_details(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_ids, event_ids}, q ->
        where(q, [e], e.event_id in ^event_ids)
    end)
  end

  defp filter_event_definitions(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [ed], ed.id == ^event_definition_id)

      {:event_definition_ids, event_definition_ids}, q ->
        where(q, [ed], ed.id in ^event_definition_ids)

      {:deployment_id, deployment_id}, q ->
        where(q, [ed], ed.deployment_id == ^deployment_id)

      {:active, active}, q ->
        where(q, [ed], ed.active == ^active)

      {:deleted_at, nil}, q ->
        where(q, [ed], is_nil(ed.deleted_at))

      {:deleted_at, _}, q ->
        where(q, [ed], is_nil(ed.deleted_at) == false)
    end)
  end

  defp filter_event_links(filter, query) do
    Enum.reduce(filter, query, fn
      {:linkage_event_ids, linkage_event_ids}, q ->
        where(q, [el], el.linkage_event_id in ^linkage_event_ids)
    end)
  end
end
