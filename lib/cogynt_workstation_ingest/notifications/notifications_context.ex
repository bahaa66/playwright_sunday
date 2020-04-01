defmodule CogyntWorkstationIngest.Notifications.NotificationsContext do
  @moduledoc """
  The Notifications context: public interface for event related functionality.
  """
  import Ecto.Query, warn: false
  alias CogyntWorkstationIngest.Repo

  alias Models.Notifications.NotificationSetting

  # ----------------------------------- #
  # --- Notification Schema Methods --- #
  # ----------------------------------- #
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
              %{}
          end
        end)
        |> Enum.to_list()
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
end
