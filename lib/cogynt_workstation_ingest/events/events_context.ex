defmodule CogyntWorkstationIngest.Events.EventsContext do
  @moduledoc """
  The Events context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Config

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

  @insert_batch_size 3_000

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
  def query_events(args, opts \\ []) do
    preload_event_details = Keyword.get(opts, :preload_event_details, false)

    query =
      Enum.reduce(args, from(e in Event), fn
        {:filter, filter}, q ->
          filter_events(filter, q)

        {:select, select}, q ->
          select(q, ^select)

        {:order_by, order_by}, q ->
          order_by(q, ^order_by)

        {:limit, limit}, q ->
          limit(q, ^limit)
      end)

    query =
      if preload_event_details do
        query
        |> preload(:event_details)
      else
        query
      end

    Repo.all(query)
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
    preload_details = Keyword.get(opts, :preload_details, false)
    include_deleted = Keyword.get(opts, :include_deleted, false)
    page = Keyword.get(opts, :page_number, 1)
    page_size = Keyword.get(opts, :page_size, 10)

    query =
      Enum.reduce(args, from(e in Event), fn
        {:filter, filter}, q ->
          filter_events(filter, q)

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

  # ---------------------------------- #
  # --- EventDetail Schema Methods --- #
  # ---------------------------------- #
  def insert_all_event_details(event_details) do
    # Postgresql protocol has a limit of maximum parameters (65535)
    Enum.chunk_every(event_details, @insert_batch_size)
    |> Enum.each(fn rows ->
      Repo.insert_all(EventDetail, rows, timeout: 60_000)
    end)
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
  def upsert_event_definition(attrs) do
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
  def get_event_definition!(id, opts \\ []) do
    preload_details = Keyword.get(opts, :preload_details, false)

    if preload_details do
      Repo.get!(EventDefinition, id)
      |> Repo.preload(:event_definition_details)
    else
      Repo.get!(EventDefinition, id)
    end
  end

  @doc """
  Returns the EventDefinition for id.
  ## Examples
      iex> get_event_definition(id)
      %EventDefinition{}
      iex> get_event_definition(invalid_id)
       nil
  """
  def get_event_definition(id, opts \\ []) do
    preload_details = Keyword.get(opts, :preload_details, false)

    if preload_details do
      Repo.get(EventDefinition, id)
      |> Repo.preload(:event_definition_details)
    else
      Repo.get(EventDefinition, id)
    end
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
        |> preload(:event_definition_details)
      else
        query
      end

    Repo.all(query)
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
  Converts an EventDefinition struct into a dropping the metadata and timestamp related fields
  """
  def remove_event_definition_virtual_fields(a, b \\ [])

  def remove_event_definition_virtual_fields([], _opts), do: []

  def remove_event_definition_virtual_fields(
        [%EventDefinition{} = event_definition | tail],
        opts
      ) do
    include_event_definition_details = Keyword.get(opts, :include_event_definition_details, false)

    if include_event_definition_details do
      event_definition_details = Map.get(event_definition, :event_definition_details, %{})

      event_definition_details =
        remove_event_definition_detail_virtual_fields(event_definition_details)

      event_definition =
        Map.put(event_definition, :event_definition_details, event_definition_details)

      [
        Map.take(event_definition, [
          :event_definition_details | EventDefinition.__schema__(:fields)
        ])
        | remove_event_definition_virtual_fields(tail, opts)
      ]
    else
      [
        Map.take(event_definition, EventDefinition.__schema__(:fields))
        | remove_event_definition_virtual_fields(tail, opts)
      ]
    end
  end

  def remove_event_definition_virtual_fields(%EventDefinition{} = event_definition, opts) do
    include_event_definition_details = Keyword.get(opts, :include_event_definition_details, false)

    if include_event_definition_details do
      event_definition_details = Map.get(event_definition, :event_definition_details, %{})

      event_definition_details =
        remove_event_definition_detail_virtual_fields(event_definition_details)

      event_definition =
        Map.put(event_definition, :event_definition_details, event_definition_details)

      Map.take(event_definition, [:event_definition_details | EventDefinition.__schema__(:fields)])
    else
      Map.take(event_definition, EventDefinition.__schema__(:fields))
    end
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
  def hard_delete_event_definition_details(event_definition_id) do
    from(details in EventDefinitionDetail,
      where: details.event_definition_id == ^event_definition_id
    )
    |> Repo.delete_all(timeout: 120_000)
  end

  def hard_delete_event_definition_details() do
    Repo.delete_all(EventDefinitionDetail, timeout: 120_000)
  end

  @doc """
  Converts an EventDefinitionDetail struct into a dropping the metadata and timestamp related fields
  """
  def remove_event_definition_detail_virtual_fields([]), do: []

  def remove_event_definition_detail_virtual_fields([
        %EventDefinitionDetail{} = event_definition_detail | tail
      ]) do
    [
      Map.take(event_definition_detail, EventDefinitionDetail.__schema__(:fields))
      | remove_event_definition_detail_virtual_fields(tail)
    ]
  end

  # -------------------------------- #
  # --- EventLink Schema Methods --- #
  # -------------------------------- #
  def insert_all_event_links(event_links) do
    # Postgresql protocol has a limit of maximum parameters (65535)
    Enum.chunk_every(event_links, @insert_batch_size)
    |> Enum.each(fn rows ->
      Repo.insert_all(EventLink, rows, timeout: 60_000)
    end)
  end

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

  # ---------------------- #
  # --- PSQL Functions --- #
  # ---------------------- #
  def insert_all_event_details_with_copy(stream_input) do
    sql = """
      COPY event_details(field_name,field_value,field_type,event_id)
      FROM STDIN (FORMAT csv, DELIMITER ';', quote E'\x01')
    """

    stream = Ecto.Adapters.SQL.stream(Repo, sql)

    Repo.transaction(fn ->
      Enum.into(stream_input, stream)
    end)
  end

  @doc """
  Calls the psql Function for inserting an event that has a
  core_id field set (crud actions)
  ## Examples
      iex> call_insert_crud_event_function(
        "ec9b2f65-3fa0-4415-8c9a-9047328cb8a3",
        "a1f76663-27b4-46b3-bad4-71b46f32eb3c",
        "39e4d640-2061-41fd-8ed5-bed579272aef",
        ~U[2021-03-10 19:07:14Z],
        nil
      )
      {:ok, %Postgrex.Result{}}
      iex> call_insert_crud_event_function(
        "ec9b2f65-3fa0-4415-8c9a-9047328cb8a3",
        "a1f76663-27b4-46b3-bad4-71b46f32eb3c",
        "39e4d640-2061-41fd-8ed5-bed579272aef",
        ~U[2021-03-10 19:07:14Z],
        nil
      )
      {:error, %Postgrex.Error{}}
  """
  def call_insert_crud_event_function(
        event_id,
        event_definition_id,
        core_id,
        occurred_at,
        deleted_at,
        deleted_by,
        event_type
      ) do
    core_id_cast =
      if is_nil(core_id) do
        "NULL"
      else
        "CAST('#{core_id}' as UUID)"
      end

    occurred_at_cast =
      if is_nil(occurred_at) do
        "NULL"
      else
        "CAST('#{occurred_at}' as TIMESTAMP)"
      end

    deleted_at_cast =
      if is_nil(deleted_at) do
        "NULL"
      else
        "CAST('#{deleted_at}' as TIMESTAMP)"
      end

    deleted_by_cast =
      if is_nil(deleted_by) do
        "NULL"
      else
        "CAST('#{deleted_by}' as VARCHAR(255))"
      end

    event_type_cast =
      if is_nil(event_type) do
        "NULL"
      else
        "CAST('#{event_type}' as VARCHAR(255))"
      end

    try do
      case Repo.query("SELECT insert_crud_event(
            CAST('#{event_id}' as UUID),
            CAST('#{event_definition_id}' as UUID),
            #{core_id_cast},
            #{occurred_at_cast},
            #{deleted_at_cast},
            #{deleted_by_cast},
            #{event_type_cast}
            )") do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "call_insert_crud_event_function/1 failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  @doc """
  Calls the psql Function for inserting link_events for
  a given event_id
  ## Examples
      iex> call_insert_event_links_function(
        "ec9b2f65-3fa0-4415-8c9a-9047328cb8a3",
        ["a1f76663-27b4-46b3-bad4-71b46f32eb3c"],
        ~U[2021-03-10 19:07:14Z]
      )
      {:ok, %Postgrex.Result{}}
      iex> call_insert_event_links_function(
        "ec9b2f65-3fa0-4415-8c9a-9047328cb8a3",
        ["a1f76663-27b4-46b3-bad4-71b46f32eb3c"],
        nil
      )
      {:error, %Postgrex.Error{}}
  """
  def call_insert_event_links_function(
        event_id,
        core_ids,
        deleted_at
      ) do
    deleted_at_cast =
      if is_nil(deleted_at) do
        "NULL"
      else
        "CAST('#{deleted_at}' as TIMESTAMP)"
      end

    try do
      case Repo.query("SELECT insert_event_links(
            CAST('#{event_id}' as UUID),
            CAST(#{inspect(core_ids)} as TEXT),
            #{deleted_at_cast}
            )") do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "call_insert_event_links_function/1 failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  def hard_delete_by_event_definition_id(event_definition_id, limit \\ 50000) do
    try do
      case Repo.query(
             "SELECT hard_delete_by_event_definition_id(
        CAST('#{event_definition_id}' as UUID),
        CAST('#{limit}' as INT)
      )",
             [],
             timeout: 120_000
           ) do
        {:ok, %Postgrex.Result{rows: rows}} ->
          if Enum.empty?(List.flatten(rows)) do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Finished deleting data linked to #{event_definition_id}."
            )

            {:ok, :success}
          else
            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleting data linked to EventDefinitionId: #{event_definition_id}. Limit: #{limit}"
            )

            hard_delete_by_event_definition_id(event_definition_id, limit)
          end

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "hard_delete_by_event_definition_id/2 failed with Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "hard_delete_by_event_definition_id/2 failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  def truncate_all_tables() do
    try do
      case Repo.query("SELECT truncate_tables('#{Config.postgres_username()}')") do
        {:ok, result} ->
          {:ok, result}

        {:error, error} ->
          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "truncate_all_tables/0 failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
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
            field_name: val.name,
            path: val.path,
            field_type: val.dataType
          })

        false ->
          create_event_definition_detail(%{
            event_definition_id: id,
            field_name: val["name"],
            path: val["path"],
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

  defp filter_event_detail_templates(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [edt], edt.event_definition_id == ^event_definition_id)
    end)
  end
end
