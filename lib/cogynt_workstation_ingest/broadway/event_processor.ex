defmodule CogyntWorkstationIngest.Broadway.EventProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the EventPipeline.
  """
  import Ecto.Query
  alias Ecto.Multi

  alias CogyntWorkstationIngest.Events.{Event, EventDetail}
  alias CogyntWorkstationIngest.Notifications.{Notification, NotificationSetting}
  alias CogyntWorkstationIngest.Repo

  @partial "$partial"
  @risk_score "_confidence"

  @doc """
  Requires event_definition field in the data map. process_event(%{}) will create a single Event
  record in the database that is assosciated with the event_definition.id. The data map
  is updated with the :event_id returned from the database.
  """
  def process_event(%{event_definition: event_definition} = data) do
    {:ok, %{id: event_id}} =
      %Event{}
      |> Event.changeset(%{event_definition_id: event_definition.id})
      |> Repo.insert()

    data = Map.put(data, :event_id, event_id)
  end

  @doc """
  Requires event, event_definition and event_id fields in the data map. Takes the
  field_name and field_value fields from the event and creates a list of event_detail
  maps. Returns an updated data map with the :event_details value having the list.
  """
  def process_event_details(
        %{event: event, event_definition: event_definition, event_id: event_id} = data
      ) do
    event_details =
      Stream.map(event, fn {field_name, field_value} ->
        field_type = event_definition.fields[field_name]

        case is_nil(field_value) do
          false ->
            field_value = encode_json(field_value)

            %{
              event_id: event_id,
              field_name: field_name,
              field_type: field_type,
              field_value: field_value
            }

          true ->
            %{}
        end
      end)
      |> Enum.to_list()

    data = Map.put(data, :event_details, event_details)
  end

  @doc """
  Requires event, event_definition and event_id fields in the data map. process_notifications(%{})
  will stream all notification_settings that are linked to the event_definition.id. On each
  notification_setting returned it will build a notification map. Finally it will return a list
  notification maps. Returns an updated data map with the field :notifications storing the list
  of notification maps.
  """
  def process_notifications(
        %{event: event, event_definition: event_definition, event_id: event_id} = data
      ) do
    case publish_notification?(event) do
      true ->
        ns_query =
          from(ns in NotificationSetting,
            where: ns.event_definition_id == type(^event_definition.id, :binary_id),
            where: ns.active == true
          )

        {:ok, notifications} =
          Repo.transaction(fn ->
            Repo.stream(ns_query)
            |> Stream.map(fn ns ->
              case Map.has_key?(event_definition.fields, ns.title) do
                true ->
                  %{
                    event_id: event_id,
                    user_id: ns.user_id,
                    # topic: event_definition.topic, TODO do we need to pass this value ?
                    tag_id: ns.tag_id,
                    title: ns.title,
                    notification_setting_id: ns.id,
                    created_at: DateTime.truncate(DateTime.utc_now(), :second),
                    updated_at: DateTime.truncate(DateTime.utc_now(), :second)
                    # TODO Optional attribute, MUST use Map.get
                    # description: Map.get(ns, :description)
                  }

                false ->
                  %{}
              end
            end)
            |> Enum.to_list()
          end)

        data = Map.put(data, :notifications, notifications)

      false ->
        data
    end
  end

  # @doc """

  # """
  # def process_elasticsearch_documents(
  #       %{event: event, event_definition: event_definition, event_id: event_id} = data
  #     ) do
  # end

  @doc """

  """
  def execute_transaction(
        %{
          event_details: event_details,
          notifications: notifications
          # elasticsearch_documents: docs
        } = data
      ) do
    result =
      Multi.new()
      |> Multi.insert_all(:insert_event_detials, EventDetail, event_details)
      |> Multi.insert_all(:insert_notifications, Notification, notifications)
      |> Repo.transaction()

    # TODO: If transaction is executed succesfully write data to elasticsearch index

    IO.inspect(result, label: "@@@ Database Transaction Result")
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp encode_json(value) do
    case String.valid?(value) do
      true ->
        value

      false ->
        Jason.encode!(value)
    end
  end

  defp publish_notification?(event) do
    partial = Map.get(event, @partial)
    risk_score = Map.get(event, @risk_score)

    if partial == nil or partial == false or risk_score > 0 do
      true
    else
      false
    end
  end
end
