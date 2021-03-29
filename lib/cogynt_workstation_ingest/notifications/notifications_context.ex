defmodule CogyntWorkstationIngest.Notifications.NotificationsContext do
  @moduledoc """
  The Notifications context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias Ecto.Adapters.SQL
  alias CogyntWorkstationIngest.Repo

  alias Models.Notifications.{NotificationSetting, Notification}

  @insert_batch_size 3_000
  @notification_table_keys [
    :id,
    :title,
    :description,
    :user_id,
    :archived_at,
    :priority,
    :assigned_to,
    :dismissed_at,
    :deleted_at,
    :event_id,
    :notification_setting_id,
    :tag_id,
    :core_id,
    :created_at,
    :updated_at
  ]

  # ------------------------------------ #
  # --- Notification Setting Methods --- #
  # ------------------------------------ #
  @doc """
  Gets a single notification_setting for the id
  ## Examples
      iex> get_notification_setting!(id)
      {:ok, %NotificationSetting{}}
      iex> get_notification_setting!(invalid_id)
      ** (Ecto.NoResultsError)
  """
  def get_notification_setting!(id), do: Repo.get!(NotificationSetting, id)

  @doc """
  Gets a single notification_setting for the id
  ## Examples
      iex> get_notification_setting(id)
      %NotificationSetting{}
      iex> get_notification_setting(invalid_id)
      nil
  """
  def get_notification_setting(id), do: Repo.get(NotificationSetting, id)

  @doc """
  Returns a single Notification_Setting struct from the query
  ## Examples
      iex> get_notification_setting_by(%{id: id})
      {:ok, %NotificationSetting{...}}
      iex> get_notification_setting_by(%{id: invalid_id})
      nil
  """
  def get_notification_setting_by(clauses),
    do: Repo.get_by(from(ns in NotificationSetting, where: is_nil(ns.deleted_at)), clauses)

  @doc """
  Returns a list of NotificationSettings that passes the list of contraints passed in
  """
  def fetch_valid_notification_settings(filters, risk_score, event_definition) do
    query_notification_settings(%{filter: filters})
    |> Enum.filter(fn ns ->
      has_event_definition_detail =
        Enum.find(event_definition.event_definition_details, fn
          %{field_name: name} ->
            name == ns.title
        end) != nil

      has_event_definition_detail and in_risk_range?(risk_score, ns.risk_range)
    end)
  end

  def fetch_invalid_notification_settings(filters, risk_score, event_definition) do
    query_notification_settings(%{filter: filters})
    |> Enum.filter(fn ns ->
      has_event_definition_detail =
        Enum.find(event_definition.event_definition_details, fn
          %{field_name: name} ->
            name == ns.title
        end) != nil

      !has_event_definition_detail or !in_risk_range?(risk_score, ns.risk_range)
    end)
  end

  @doc """
  Querys NotificationSettings based on the filter args
  ## Examples
      iex> query_notification_settings(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      [%NotificationSetting{}, %NotificationSetting{}]
  """
  def query_notification_settings(args) do
    query =
      Enum.reduce(args, from(ns in NotificationSetting), fn
        {:filter, filter}, q ->
          filter_notification_settings(filter, q)

        {:select, select}, q ->
          select(q, ^select)

        {:order_by, order_by}, q ->
          order_by(q, ^order_by)

        {:limit, limit}, q ->
          limit(q, ^limit)
      end)

    Repo.all(query)
  end

  @doc """
  Takes a list of NotificationSetting Structs and returns a list of maps with
  the Metadata and Timestamp fields dropped
  """
  def remove_notification_setting_virtual_fields([]), do: []

  def remove_notification_setting_virtual_fields([
        %NotificationSetting{} = notification_setting | tail
      ]) do
    [
      Map.take(notification_setting, NotificationSetting.__schema__(:fields))
      | remove_notification_setting_virtual_fields(tail)
    ]
  end

  # ---------------------------- #
  # --- Notification Methods --- #
  # ---------------------------- #

  @doc """
  Returns a single Notification struct from the query
  ## Examples
      iex> get_notification_by(%{id: id})
      {:ok, %Notification{...}}
      iex> get_notification_by(%{id: invalid_id})
      nil
  """
  def get_notification_by(clauses),
    do: Repo.get_by(Notification, clauses)

  @doc """
  Querys Notifications based on the filter args
  ## Examples
      iex> query_notifications(
        %{
          filter: %{
            event_definition_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      [%Notifications{}, %Notifications{}]
  """
  def query_notifications(args) do
    query =
      Enum.reduce(args, from(n in Notification), fn
        {:filter, filter}, q ->
          filter_notifications(filter, q)

        {:select, select}, q ->
          select(q, ^select)

        {:order_by, order_by}, q ->
          order_by(q, ^order_by)

        {:limit, limit}, q ->
          limit(q, ^limit)
      end)

    Repo.all(query)
  end

  @doc """
  Returns a list of the %Notification{} stucts that were inserted.
  ## Examples
      iex> insert_all_notifications([%Notification{}, returning: [:id])
      {20, [%Notification{...}]}
  """
  def insert_all_notifications(notifications, opts \\ []) when is_list(notifications) do
    returning = Keyword.get(opts, :returning, [:id])
    on_conflict = Keyword.get(opts, :on_conflict, :nothing)
    conflict_target = Keyword.get(opts, :conflict_target, :id)

    if Enum.empty?(notifications) do
      {0, []}
    else
      # Postgresql protocol has a limit of maximum parameters (65535)
      Enum.chunk_every(notifications, @insert_batch_size)
      |> Enum.reduce({0, []}, fn rows, {acc_count, acc_notifications} ->
        {count, result} =
          Repo.insert_all(Notification, rows,
            returning: returning,
            on_conflict: on_conflict,
            conflict_target: conflict_target,
            timeout: 60_000
          )

        if is_nil(result) do
          {acc_count + count, acc_notifications}
        else
          {acc_count + count, acc_notifications ++ result}
        end
      end)
    end
  end

  @doc """
  Returns a page of notifications by the args and page
  ## Examples
      iex> get_page_of_notifications(
        %{
          filter: %{
            notifications_setting_id: "dec1dcda-7f32-11ea-bc55-0242ac130003"
          }
        },
        page_number: 1
      )
      %Scrivener.Page{
        entries: [%Notification{}],
        page_number: 1,
        page_size: 500,
        total_entries: 1200,
        total_pages: 3
      }
  """
  def get_page_of_notifications(args, opts \\ []) do
    include_deleted = Keyword.get(opts, :include_deleted, false)
    page = Keyword.get(opts, :page_number, 1)
    page_size = Keyword.get(opts, :page_size, 10)

    query =
      Enum.reduce(args, from(n in Notification), fn
        {:filter, filter}, q ->
          filter_notifications(filter, q)

        {:select, select}, q ->
          select(q, ^select)
      end)

    if include_deleted do
      query
    else
      query
      |> where([n], is_nil(n.deleted_at))
    end
    |> order_by([n], desc: n.created_at, asc: n.id)
    |> Repo.paginate(page: page, page_size: page_size)
  end

  @doc """
  Bulk updates a list of notifications by filter and it also allows you to select the
  columns you want to return.
  ## Examples
      iex> update_notifications(
        %{
          filter: %{
            notificiation_setting_id: "c1607818-7f32-11ea-bc55-0242ac130003"
          }
        }
      )
      {2, [%Notification{}, %Notification{}]}
  """
  def update_notifcations(args, set: set) do
    query =
      Enum.reduce(args, from(n in Notification), fn
        {:filter, filter}, q ->
          filter_notifications(filter, q)

        {:select, select}, q ->
          select(q, ^select)
      end)

    Repo.update_all(query, set: set)
  end

  @doc """
  Updates an NotificationSetting.
  ## Examples
      iex> update_notification_setting(notification_setting, %{field: new_value})
      {:ok, %NotificationSetting{}}
      iex> update_notification_setting(notification_setting, %{field: bad_value})
      {:error, ...}
  """
  def update_notification_setting(%NotificationSetting{} = notification_setting, attrs) do
    notification_setting
    |> NotificationSetting.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Takes a list of Notification Structs and returns a list of maps with
  the Metadata and Timestamp fields dropped
  """
  def remove_notification_virtual_fields([]), do: []

  def remove_notification_virtual_fields([%Notification{} = notification | tail]) do
    [
      Map.take(notification, Notification.__schema__(:fields))
      | remove_notification_virtual_fields(tail)
    ]
  end

  def remove_notification_virtual_fields([_ | tail]) do
    remove_notification_virtual_fields(tail)
  end

  # ---------------------- #
  # --- PSQL Functions --- #
  # ---------------------- #
  def insert_all_notifications_with_copy(stream_input) do
    # 1) Create Temp Table
    temp_table_name = "notifications_#{Ecto.UUID.generate()}"

    try do
      case Repo.query(
             "SELECT create_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))"
           ) do
        {:ok, _result} ->
          # 2) Copy data into temp table
          statement = "COPY public.\"#{temp_table_name}\"(
            id,
            title,
            description,
            user_id,
            archived_at,
            priority,
            assigned_to,
            dismissed_at,
            deleted_at,
            event_id,
            notification_setting_id,
            tag_id,
            core_id,
            created_at,
            updated_at
          )
          FROM STDIN CSV DELIMITER ';' NULL AS 'null'"

          stream = SQL.stream(Repo, statement)

          case Repo.transaction(fn ->
                 Enum.into(stream_input, stream)
               end) do
            {:ok, _changes} ->
              # 3) Merge temp table into notifications table, handle conflicts, drop temp table
              try do
                case Repo.query("SELECT merge_notifications_from_unlogged_table(
                CAST('#{temp_table_name}' as regclass)
              )") do
                  {:ok, result} ->
                    Repo.query(
                      "SELECT drop_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))"
                    )

                    # 4) This is the Final return object if all is success
                    {:ok, result}

                  {:error, error} ->
                    CogyntLogger.error(
                      "#{__MODULE__}",
                      "insert_all_notifications_with_copy/1 Failed to merge Unlogged and Actual notification tables. Error: #{
                        inspect(error)
                      }"
                    )

                    Repo.query(
                      "SELECT drop_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))"
                    )

                    {:error, error}
                end
              rescue
                error ->
                  CogyntLogger.error(
                    "#{__MODULE__}",
                    "insert_all_notifications_with_copy/1 Failed to merge Unlogged and Actual notification tables. Error: #{
                      inspect(error)
                    }"
                  )

                  Repo.query(
                    "SELECT drop_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))"
                  )

                  {:error, :internal_server_error}
              end

            {:error, error} ->
              CogyntLogger.error(
                "#{__MODULE__}",
                "insert_all_notifications_with_copy/1 Failed at the COPY step. Error: #{
                  inspect(error)
                }"
              )

              {:error, :internal_server_error}
          end

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "insert_all_notifications_with_copy/1 Failed to create unlogged table #{
              temp_table_name
            } Error: #{inspect(error)} "
          )

          Repo.query(
            "SELECT drop_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))"
          )

          {:error, error}
      end
    rescue
      error ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "insert_all_notifications_with_copy/1 Failed to create unlogged table #{temp_table_name}. Error: #{
            inspect(error)
          }"
        )

        Repo.query("SELECT drop_unlogged_notifications_table(CAST('#{temp_table_name}' as TEXT))")
        {:error, :internal_server_error}
    end
  end

  def map_postgres_results(postgres_results) do
    Enum.reduce(postgres_results, [], fn rows, acc_0 ->
      rows_result =
        Enum.reduce(rows, [], fn row, acc_1 ->
          keys = @notification_table_keys

          values =
            if is_tuple(row) do
              Tuple.to_list(row)
            else
              row
            end
            |> Enum.reduce([], fn column, acc ->
              case Ecto.UUID.cast(column) do
                {:ok, uuid} ->
                  acc ++ [uuid]

                _ ->
                  acc ++ [column]
              end
            end)

          notifications_enum = Enum.zip(keys, values) |> Enum.into(%{})

          acc_1 ++ [notifications_enum]
        end)

      acc_0 ++ rows_result
    end)
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp in_risk_range?(risk_score, risk_range) do
    with true <- risk_score > 0,
         converted_risk_score <- trunc(Float.round(risk_score * 100)),
         min_risk_range <- Enum.min(risk_range),
         max_risk_range <- Enum.max(risk_range) do
      if converted_risk_score >= min_risk_range and converted_risk_score <= max_risk_range do
        true
      else
        false
      end
    else
      # risk_score == 0
      false ->
        if Enum.min(risk_range) > 0 do
          false
        else
          # risk_score == 0 and min_range == 0
          true
        end

      _ ->
        CogyntLogger.warn("#{__MODULE__}", "Risk Range validation failed")
        false
    end
  end

  defp filter_notifications(filter, query) do
    Enum.reduce(filter, query, fn
      {:notification_ids, notification_ids}, q ->
        where(q, [n], n.id in ^notification_ids)

      {:notification_setting_id, notification_setting_id}, q ->
        where(q, [n], n.notification_setting_id == ^notification_setting_id)

      {:notification_setting_ids, notification_setting_ids}, q ->
        where(q, [n], n.notification_setting_id in ^notification_setting_ids)

      {:core_id, core_id}, q ->
        where(q, [n], n.core_id == ^core_id)

      {:event_ids, event_ids}, q ->
        where(q, [n], n.event_id in ^event_ids)

      {:deleted_at, nil}, q ->
        where(q, [n], is_nil(n.deleted_at))

      {:deleted_at, _}, q ->
        where(q, [n], is_nil(n.deleted_at) == false)
    end)
  end

  defp filter_notification_settings(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [ns], ns.event_definition_id == ^event_definition_id)

      {:active, active}, q ->
        where(q, [ns], ns.active == ^active)

      {:deleted_at, nil}, q ->
        where(q, [ns], is_nil(ns.deleted_at))

      {:deleted_at, _}, q ->
        where(q, [ns], is_nil(ns.deleted_at) == false)
    end)
  end
end
