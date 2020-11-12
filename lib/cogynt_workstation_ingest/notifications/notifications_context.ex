defmodule CogyntWorkstationIngest.Notifications.NotificationsContext do
  @moduledoc """
  The Notifications context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias Ecto.Multi
  alias CogyntWorkstationIngest.Repo

  alias Models.Notifications.{NotificationSetting, Notification}

  @delete Application.get_env(:cogynt_workstation_ingest, :core_keys)[:delete]

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
  Hard deletes many notification settings and removes them from the database.

  ## Examples
      iex> hard_delete_notification_settings(%{
        filter: %{event_ids: ["9a9055f1-98b6-42a9-8145-7a4ca09cab23"]}
      })
      {1, nil | [%Notification{}]}
  """
  def hard_delete_notification_settings(args) do
    Enum.reduce(args, from(ns in NotificationSetting), fn
      {:filter, filter}, q ->
        filter_notification_settings(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.delete_all(timeout: 120_000)
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
  Returns a list of the %Notification{} stucts that were inserted.
  ## Examples
      iex> bulk_insert_notifications([%Notification{}, returning: [:id])
      {20, [%Notification{...}]}
  """
  def bulk_insert_notifications(notifications, opts \\ []) when is_list(notifications) do
    returning = Keyword.get(opts, :returning, [])

    Repo.insert_all(Notification, notifications, returning: returning)
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
  Hard deletes many notifications and removes them from the database.
  ## Examples
      iex> hard_delete_notifications(%{
          filter: %{
            event_ids: ["9a9055f1-98b6-42a9-8145-7a4ca09cab23"]
          }
        })
      {1, nil | [%Notification{}]}
  """
  def hard_delete_notifications(args) do
    Enum.reduce(args, from(n in Notification), fn
      {:filter, filter}, q ->
        filter_notifications(filter, q)

      {:select, select}, q ->
        select(q, ^select)
    end)
    |> Repo.delete_all(timeout: 120_000)
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

  # ------------------------------- #
  # --- Event Processor Methods --- #
  # ------------------------------- #
  @doc """
  Formats a list of notifications to be created for an event_definition and event_id.
  ## Examples
      iex> process_notifications(%{event_definition: event_definition, event_id: event_id, risk_score: risk_score})
      {:ok, [%{}, %{}]} || {:ok, nil}
      iex> process_notifications(%{field: bad_value})
      {:error, reason}
  """
  def process_notifications(%{
        event_definition: event_definition,
        event_id: event_id,
        risk_score: risk_score
      }) do
    result =
      from(ns in NotificationSetting,
        where: ns.event_definition_id == type(^event_definition.id, :binary_id),
        where: ns.active == true and is_nil(ns.deleted_at)
      )
      |> Repo.all()
      |> Enum.map(fn ns ->
        has_event_definition_detail =
          Enum.find(event_definition.event_definition_details, fn
            %{field_name: name} ->
              name == ns.title
          end) != nil

        if in_risk_range?(risk_score, ns.risk_range) and has_event_definition_detail do
          %{
            event_id: event_id,
            user_id: ns.user_id,
            assigned_to: ns.assigned_to,
            tag_id: ns.tag_id,
            title: ns.title,
            notification_setting_id: ns.id,
            created_at: DateTime.truncate(DateTime.utc_now(), :second),
            updated_at: DateTime.truncate(DateTime.utc_now(), :second)
          }
        else
          nil
        end
      end)
      |> Enum.to_list()
      |> Enum.filter(& &1)

    if Enum.empty?(result) do
      {:ok, nil}
    else
      {:ok, result}
    end
  end

  def insert_all_notifications_multi(multi \\ Multi.new(), notifications, opts \\ []) do
    returning = Keyword.get(opts, :returning, [])

    multi
    |> Multi.insert_all(:insert_notifications, Notification, notifications, returning: returning)
  end

  def update_all_notifications_multi(multi \\ Multi.new(), %{
        delete_event_ids: delete_event_ids,
        action: action,
        event_id: event_id
      }) do
    case is_nil(delete_event_ids) or Enum.empty?(delete_event_ids) do
      true ->
        multi

      false ->
        # If action is a delete we want to leave all notifications
        # marked as deleted. Event the ones created for the current newest event.
        deleted_at =
          if action == @delete do
            DateTime.truncate(DateTime.utc_now(), :second)
          else
            nil
          end

        n_query =
          from(n in Notification,
            where: n.event_id in ^delete_event_ids,
            select: %{
              event_id: n.event_id,
              user_id: n.user_id,
              tag_id: n.tag_id,
              id: n.id,
              title: n.title,
              notification_setting_id: n.notification_setting_id,
              created_at: n.created_at,
              updated_at: n.updated_at,
              assigned_to: n.assigned_to,
              deleted_at: n.deleted_at
            }
          )

        multi
        |> Multi.update_all(:update_notifications, n_query,
          set: [event_id: event_id, deleted_at: deleted_at]
        )
    end
  end

  def run_multi_transaction(multi) do
    Repo.transaction(multi, timeout: 120_000)
  end

  def in_risk_range?(risk_score, risk_range) do
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

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp filter_notifications(filter, query) do
    Enum.reduce(filter, query, fn
      {:notification_ids, notification_ids}, q ->
        where(q, [n], n.id in ^notification_ids)

      {:notification_setting_id, notification_setting_id}, q ->
        where(q, [n], n.notification_setting_id == ^notification_setting_id)

      {:event_ids, event_ids}, q ->
        where(q, [n], n.event_id in ^event_ids)
    end)
  end

  defp filter_notification_settings(filter, query) do
    Enum.reduce(filter, query, fn
      {:event_definition_id, event_definition_id}, q ->
        where(q, [ns], ns.event_definition_id == ^event_definition_id)
    end)
  end
end
