defmodule CogyntWorkstationIngest.Notifications.NotificationsContext do
  @moduledoc """
  The Notifications context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo

  alias Models.Notifications.{NotificationSetting, Notification}

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
  def get_notification_by(clauses), do: Repo.get_by(Notification, clauses)

  @doc """
  Returns a list of the %Notification{} stucts that were inserted.
  ## Examples
      iex> bulk_insert_notifications([])
      {:ok, [%Notification{...}]}
  """
  def bulk_insert_notifications(notifications) when is_list(notifications) do
    {_count, updated_notifications} =
      Repo.insert_all(Notification, notifications,
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

    {:ok, updated_notifications}
  end

  # ------------------------------- #
  # --- Event Processor Methods --- #
  # ------------------------------- #
  @doc """
  Formats a list of notifications to be created for an event_definition and event_id.
  ## Examples
      iex> process_notifications(%{event_definition: event_definition, event_id: event_id})
      {:ok, [%{}, %{}]} || {:ok, nil}
      iex> process_notifications(%{field: bad_value})
      {:error, reason}
  """
  def process_notifications(%{event_definition: event_definition, event_id: event_id}) do
    ns_query =
      from(ns in NotificationSetting,
        where: ns.event_definition_id == type(^event_definition.id, :binary_id),
        where: ns.active == true
      )

    {status, result} =
      Repo.transaction(fn ->
        Repo.stream(ns_query)
        |> Stream.map(fn ns ->
          case Map.has_key?(event_definition.fields, ns.title) do
            true ->
              %{
                event_id: event_id,
                user_id: ns.user_id,
                # topic: event_definition.topic, TODO: do we need to pass this value ??
                tag_id: ns.tag_id,
                title: ns.title,
                notification_setting_id: ns.id,
                created_at: DateTime.truncate(DateTime.utc_now(), :second),
                updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                # TODO: do we need to pass this value ??
                # Optional attribute, MUST use Map.get
                # description: Map.get(ns, :description)
              }

            false ->
              nil
          end
        end)
        |> Enum.to_list()
        |> Enum.filter(& &1)
      end)

    case status do
      :ok ->
        if Enum.empty?(result) do
          {:ok, nil}
        else
          {:ok, result}
        end
    end
  end

  def update_notification_setting_notifications(
        %NotificationSetting{
          id: id,
          tag_id: tag_id,
          deleted_at: deleted_at
        },
        callback
      ) do
    Repo.transaction(fn ->
      from(n in Notification, where: n.notification_setting_id == ^id, select: n.id)
      |> where(notification_setting_id: ^id)
      |> Repo.stream(max_rows: 2000)
      |> Stream.chunk_every(2000)
      |> Task.async_stream(
        fn notification_ids ->
          case Repo.update_all(
                 from(n in Notification,
                   where: n.id in ^notification_ids,
                   select: %{
                     event_id: n.event_id,
                     user_id: n.user_id,
                     tag_id: n.tag_id,
                     id: n.id,
                     title: n.title,
                     notification_setting_id: n.notification_setting_id,
                     created_at: n.created_at,
                     updated_at: n.updated_at
                   }
                 ),
                 set: [tag_id: tag_id, deleted_at: deleted_at]
               ) do
            {_count, updated_notifications} ->
              callback.(updated_notifications)
          end
        end,
        max_concurrency: 10
      )
      |> Stream.run()
    end)
  end
end
