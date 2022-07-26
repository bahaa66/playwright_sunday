defmodule CogyntWorkstationIngest.Events.EventsContext do
  @moduledoc """
  The Events context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Config

  alias Models.Events.{
    Event,
    EventDefinition,
    EventLink,
    EventDefinitionDetail,
    EventHistory
  }

  alias Models.EventDetailTemplates.{
    EventDetailTemplate,
    EventDetailTemplateGroup,
    EventDetailTemplateGroupItem
  }

  def run_multi_transaction(multi) do
    Repo.transaction(multi)
  end

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
            event_definition_hash_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      [%Event{}, %Event{}]
  """
  def query_events(args) do
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
    |> Repo.all()
  end

  @doc """
  Paginates through Events based on the event_definition_hash_id.
  Returns the page_number as a %Scrivener.Page{} object.
  ## Examples
      iex> get_page_of_events(
        %{
          filter: %{
            event_definition_hash_id: "dec1dcda-7f32-11ea-bc55-0242ac130003"
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
    page = Keyword.get(opts, :page_number, 1)
    page_size = Keyword.get(opts, :page_size, 10)

    Enum.reduce(args, from(e in Event), fn
      {:filter, filter}, q ->
        filter_events(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> order_by([e], desc: e.created_at, asc: e.core_id)
    |> Repo.paginate(page: page, page_size: page_size)
  end

  @doc """
  Bulk updates many events.
  ## Examples
      iex> update_events(
        %{
          filter: %{
            event_definition_hash_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      {2, [%Event{}, %Event{}]}
  """
  def update_events(args, set: set) do
    Enum.reduce(args, from(e in Event), fn
      {:filter, filter}, q ->
        filter_events(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.update_all(set: set)
  end

  def upsert_all_events_multi(multi, events \\ [], opts \\ [])

  def upsert_all_events_multi(multi, [], _opts), do: multi

  def upsert_all_events_multi(multi, events, opts) do
    returning = Keyword.get(opts, :returning, [:core_id])
    on_conflict = Keyword.get(opts, :on_conflict, :nothing)
    conflict_target = Keyword.get(opts, :conflict_target, [:core_id])

    multi
    |> Multi.insert_all(:upsert_events, Event, events,
      returning: returning,
      on_conflict: on_conflict,
      conflict_target: conflict_target,
      timeout: 60_000
    )
  end

  def delete_all_events_multi(multi, core_ids \\ [])

  def delete_all_events_multi(multi, []), do: multi

  def delete_all_events_multi(multi, core_ids) do
    multi
    |> Multi.delete_all(:delete_events, from(e in Event, where: e.core_id in ^core_ids))
  end

  # ----------------------------------- #
  # --- EventHistory Schema Methods --- #
  # ----------------------------------- #

  def upsert_all_event_history_multi(multi, event_history \\ [], opts \\ [])

  def upsert_all_event_history_multi(multi, [], _opts), do: multi

  def upsert_all_event_history_multi(multi, event_history, opts) do
    returning = Keyword.get(opts, :returning, [:core_id])
    on_conflict = Keyword.get(opts, :on_conflict, :nothing)
    conflict_target = Keyword.get(opts, :conflict_target, [:core_id, :version, :crud])

    multi
    |> Multi.insert_all(:upsert_event_history, EventHistory, event_history,
      returning: returning,
      on_conflict: on_conflict,
      conflict_target: conflict_target,
      timeout: 60_000
    )
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
  Will create the EventDefinition if no record is found for the event_definition_hash_id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_event_definition_v2(%{field: value})
      {:ok, %EventDefinition{}}
      iex> upsert_event_definition_v2(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  def upsert_event_definition_v2(attrs) do
    case get_event_definition(attrs.id) do
      nil ->
        create_event_definition(attrs)

      %EventDefinition{} = event_definition ->
        update_event_definition(event_definition, attrs)
    end
  end

  @doc """
  Will create the EventDefinition if no record is found for the event_definition_hash_id.
  If a record is found it updates the record with the new attrs.
  ## Examples
      iex> upsert_event_definition(%{field: value})
      {:ok, %EventDefinition{}}
      iex> upsert_event_definition(%{field: bad_value})
      {:error, %Ecto.Changeset{}}
  """
  # *** TODO: This can be deprecated once Authoring 1.0 is no longer supported ***
  ### WARNING: This is no longer working after Admin Redesign Change. Authoring 1 ingestion will
  # not work ###
  def upsert_event_definition(attrs) do
    case get_event_definition_by(%{
           id: attrs.id,
           deployment_id: attrs.deployment_id
         }) do
      nil ->
        new_event_def = create_event_definition(attrs)

        case new_event_def do
          {:ok, %EventDefinition{event_definition_details_id: event_definition_details_id}} ->
            if Map.has_key?(attrs, :fields) do
              process_event_definition_detail_fields(event_definition_details_id, attrs.fields)
              |> insert_all_event_details()
            end

            new_event_def

          _ ->
            new_event_def
        end

      %EventDefinition{} = event_definition ->
        updated_event_def = update_event_definition(event_definition, attrs)

        case updated_event_def do
          {:ok, %EventDefinition{id: id}} ->
            # Delete all EventDefinitionDetails for event_definition_hash_id
            hard_delete_event_definition_details(id)
            # Create new EventDefinitionDetails for event_definition_hash_id
            if Map.has_key?(attrs, :fields) do
              process_event_definition_detail_fields(id, attrs.fields)
              |> insert_all_event_details()
            end

            updated_event_def

          _ ->
            updated_event_def
        end
    end
  end

  # ***********************************************************************

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

  @spec get_event_definition_by(any) :: any
  @doc """
  Returns a single EventDefinition struct from the query
  ## Examples
      iex> get_event_definition_by(%{id: id})
      %EventDefinition{...}
      iex> get_event_definition_by(%{id: invalid_id})
      nil
  """
  def get_event_definition_by(clauses),
    do: Repo.get_by(EventDefinition, clauses)

  @doc """
  Query EventDefinitions
  ## Examples
      iex> query_event_definitions(
        %{
          filter: %{
            event_definition_hash_id: "c1607818-7f32-11ea-bc55-0242ac130003"
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
            event_definition_hash_id: "c1607818-7f32-11ea-bc55-0242ac130003"
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

  def delete_event_definitions(opts \\ []) do
    Enum.reduce(opts, from(ed in EventDefinition), fn
      {:select, s}, acc -> acc |> select(^s)
    end)
    |> Repo.delete_all()
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
  %EventDetailTemplate{} for the event_definition_hash_id to be deleted
    ## Examples
      iex> delete_event_definition_event_detail_templates(%{id: event_definition_hash_id}, deleted_at)
      {count, [%EventDetailTemplate{...}]}
      iex> delete_event_definition_event_detail_templates(%{id: invalid_id}, deleted_at)
      nil
  """
  def delete_event_definition_event_detail_templates(%{id: event_definition_hash_id}, deleted_at) do
    queryable =
      from(et in EventDetailTemplate)
      |> where([et], et.event_definition_hash_id == ^event_definition_hash_id)
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

  def insert_all_event_details(event_details) do
    Repo.insert_all(EventDefinitionDetail, event_details)
  end

  @doc """
  Will return all the EventDefintionDetails for the given event_definition_details_id
  """
  def get_event_definition_details(event_definition_details_id) do
    from(details in EventDefinitionDetail,
      where: details.event_definition_details_id == ^event_definition_details_id
    )
    |> Repo.all()
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
  def hard_delete_event_definition_details(event_definition_details_id) do
    from(details in EventDefinitionDetail,
      where: details.event_definition_details_id == ^event_definition_details_id
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

  def process_event_definition_detail_fields(event_def_details_id, fields) do
    Enum.reduce(fields, [], fn {key, val}, acc ->
      case is_atom(key) do
        true ->
          field_type =
            cond do
              val.dataType == "poly" ->
                "geo"

              val.dataType == "poly-array" or
                  val.dataType == "geo-array" ->
                "geo-array"

              true ->
                val.dataType
            end

          acc ++
            [
              %{
                event_definition_details_id: event_def_details_id,
                field_name: val.name,
                path: val.path,
                field_type: field_type
              }
            ]

        false ->
          field_type =
            cond do
              val["dataType"] == "poly" ->
                "geo"

              val["dataType"] == "poly-array" or
                  val["dataType"] == "geo-array" ->
                "geo-array"

              true ->
                val["dataType"]
            end

          acc ++
            [
              %{
                event_definition_details_id: event_def_details_id,
                field_name: val["name"],
                path: val["path"],
                field_type: field_type
              }
            ]
      end
    end)
  end

  def process_event_definition_detail_fields_v2(event_def_details_id, fields) do
    Enum.reduce(fields, [], fn details, acc ->
      field_type =
        cond do
          details.dataType == "poly" ->
            "geo"

          details.dataType == "poly-array" or
              details.dataType == "geo-array" ->
            "geo-array"

          true ->
            details.dataType
        end

      acc ++
        [
          %{
            event_definition_details_id: event_def_details_id,
            field_name: details.name,
            path: details.path,
            field_type: field_type
          }
        ]
    end)
  end

  # -------------------------------- #
  # --- EventLink Schema Methods --- #
  # -------------------------------- #
  def insert_all_event_links(event_links) do
    Repo.insert_all(EventLink, event_links, timeout: 60_000)
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

  def upsert_all_event_links_multi(multi, event_links \\ [], opts \\ [])

  def upsert_all_event_links_multi(multi, [], _opts), do: multi

  def upsert_all_event_links_multi(multi, event_links, _opts) do
    multi
    |> Multi.insert_all(:upsert_event_links, EventLink, event_links, timeout: 60_000)
  end

  def delete_all_event_links_multi(multi, core_ids \\ [])

  def delete_all_event_links_multi(multi, []), do: multi

  def delete_all_event_links_multi(multi, core_ids) do
    multi
    |> Multi.delete_all(
      :delete_event_links,
      from(el in EventLink,
        where: el.link_core_id in ^core_ids or el.entity_core_id in ^core_ids
      )
    )
  end

  # ---------------------- #
  # --- PSQL Functions --- #
  # ---------------------- #
  def hard_delete_by_event_definition_hash_id(event_definition_hash_id, limit \\ 10000) do
    try do
      case Repo.query(
             "SELECT hard_delete_by_event_definition_hash_id(
        CAST('#{event_definition_hash_id}' as UUID),
        CAST('#{limit}' as INT)
      )",
             [],
             timeout: :infinity
           ) do
        {:ok, %Postgrex.Result{rows: rows}} ->
          if Enum.empty?(List.flatten(rows)) do
            CogyntLogger.info(
              "#{__MODULE__}",
              "Finished deleting data linked to #{event_definition_hash_id}."
            )

            {:ok, :success}
          else
            CogyntLogger.info(
              "#{__MODULE__}",
              "Deleting data linked to EventDefinitionHashId: #{event_definition_hash_id}. Limit: #{limit}"
            )

            hard_delete_by_event_definition_hash_id(event_definition_hash_id, limit)
          end

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "hard_delete_by_event_definition_hash_id/2 failed with Error: #{inspect(error)}"
          )

          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "hard_delete_by_event_definition_hash_id/2 failed with Error: #{inspect(error)}"
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

  def execute_ingest_bulk_insert_function(bulk_transactional_data) do
    try do
      remove_notification_core_ids =
        Enum.uniq(
          bulk_transactional_data.delete_core_id ++
            bulk_transactional_data.pg_notifications_delete
        )

      remove_event_link_core_ids =
        Enum.uniq(
          bulk_transactional_data.delete_core_id ++ bulk_transactional_data.pg_event_links_delete
        )

      IO.inspect(Enum.count(bulk_transactional_data.pg_event_list), label: "EVENT COUNT")

      IO.inspect(Enum.count(bulk_transactional_data.pg_event_history),
        label: "EVENT HISTORY COUNT"
      )

      IO.inspect(Enum.count(bulk_transactional_data.pg_event_links), label: "EVENT LINKS COUNT")

      IO.inspect(Enum.count(bulk_transactional_data.pg_notifications),
        label: "NOTIFICATIONS COUNT"
      )

      IO.inspect(bulk_transactional_data.pg_event_list, label: "PG_EVENT_LIST")

      events_temp_table_name = "events_" <> "#{Ecto.UUID.generate()}"

      events_sql = """
      CREATE TEMP UNLOGGED TABLE #{events_temp_table_name} (
        core_id uuid NOT NULL,
        occurred_at timestamp(0) NULL,
        risk_score int4 NULL,
        event_details jsonb NOT NULL DEFAULT '{}'::jsonb,
        created_at timestamp(0) NOT NULL,
        updated_at timestamp(0) NOT NULL,
        event_definition_hash_id uuid NULL
      );
      COPY #{events_temp_table_name}(core_id, occurred_at, risk_score, event_details, created_at, updated_at, event_definition_hash_id)
      FROM STDIN (FORMAT csv, DELIMITER ';', quote E'\x01');

      INSERT INTO events(core_id, occurred_at, risk_score, event_details, created_at, updated_at, event_definition_hash_id)
      SELECT * FROM #{events_temp_table_name}
      ON CONFLICT (core_id)
      DO UPDATE SET
          occurred_at = EXCLUDED.occurred_at,
          risk_score = EXCLUDED.risk_score,
          event_details = EXCLUDED.event_details,
          updated_at = EXCLUDED.updated_at,
          event_definition_hash_id = EXCLUDED.event_definition_hash_id;
      """

      # events_sql = """
      #   COPY events(core_id, occurred_at, risk_score, event_details, created_at, updated_at, event_definition_hash_id)
      #   FROM STDIN (FORMAT csv, DELIMITER ';', quote E'\x01')
      #   ON CONFLICT (core_id)
      #   DO UPDATE SET
      #     occurred_at = EXCLUDED.occurred_at,
      #     risk_score = EXCLUDED.risk_score,
      #     event_details = EXCLUDED.event_details,
      #     updated_at = EXCLUDED.updated_at,
      #     event_definition_hash_id = EXCLUDED.event_definition_hash_id;
      # """

      events_stream = Ecto.Adapters.SQL.stream(Repo, events_sql)

      Repo.transaction(fn ->
        Enum.into(bulk_transactional_data.pg_event_list, events_stream)
      end)
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "execute_ingest_bulk_insert_function/0 failed with Error: #{inspect(error)}"
        )

        {:error, :internal_server_error}
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp filter_events(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_hash_id, event_definition_hash_id}, q ->
        where(q, [e], e.event_definition_hash_id == ^event_definition_hash_id)

      {:event_definition_hash_ids, event_definition_hash_ids}, q ->
        where(q, [e], e.event_definition_hash_id in ^event_definition_hash_ids)

      {:core_ids, core_ids}, q ->
        where(q, [e], e.core_id in ^core_ids)
    end)
  end

  defp filter_event_definitions(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_hash_id, event_definition_hash_id}, q ->
        where(q, [ed], ed.id == ^event_definition_hash_id)

      {:event_definition_hash_ids, event_definition_hash_ids}, q ->
        where(q, [ed], ed.id in ^event_definition_hash_ids)

      {:event_definition_id, event_definition_id}, q ->
        where(q, [ed], ed.event_definition_id == ^event_definition_id)

      {:event_definition_ids, event_definition_ids}, q ->
        where(q, [ed], ed.event_definition_id in ^event_definition_ids)

      {:active, active}, q ->
        where(q, [ed], ed.active == ^active)
    end)
  end

  defp filter_event_links(filter, query) do
    Enum.reduce(filter, query, fn
      {:link_core_ids, link_core_ids}, q ->
        where(q, [el], el.link_core_id in ^link_core_ids)
    end)
  end
end
