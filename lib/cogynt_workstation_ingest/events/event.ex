defmodule CogyntWorkstationIngest.Events.Event do
  @moduledoc """
  The Event ecto schema
  """
  use Ecto.Schema
  import Ecto.Changeset
  alias CogyntWorkstationIngest.Events.{EventDefinition, EventDetail, EventLink}
  alias CogyntWorkstationIngest.Notifications.Notification

  @primary_key {:id, :binary_id, autogenerate: true}
  @derive {Phoenix.Param, key: :id}

  schema "events" do
    belongs_to :event_definition, EventDefinition,
      foreign_key: :event_definition_id,
      type: Ecto.UUID

    has_many :event_details, EventDetail
    has_many :notifications, Notification
    has_many :event_links, EventLink, foreign_key: :parent_event_id, references: :id

    field :deleted_at, :utc_datetime
    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  def changeset(events, attrs) do
    events
    |> cast(attrs, [:event_definition_id])
    |> validate_required([:event_definition_id])
    |> foreign_key_constraint(:event_definition_id)
  end
end
