defmodule CogyntWorkstationIngest.Events.EventLink do
  use Ecto.Schema
  import Ecto.Changeset
  alias CogyntWorkstationIngest.Events.Event

  @primary_key {:id, :binary_id, autogenerate: true}
  @derive {Phoenix.Param, key: :id}

  schema "event_links" do
    belongs_to :parent_event, Event, foreign_key: :parent_event_id, type: Ecto.UUID
    belongs_to :child_event, Event, foreign_key: :child_event_id, type: Ecto.UUID
    belongs_to :linkage_event, Event, foreign_key: :linkage_event_id, type: Ecto.UUID

    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  def changeset(details, attr) do
    details
    |> cast(attr, [:parent_event_id, :child_event_id, :linkage_event_id])
    |> validate_required([:parent_event_id, :child_event_id, :linkage_event_id])
    |> foreign_key_constraint(:parent_event_id)
    |> foreign_key_constraint(:child_event_id)
    |> foreign_key_constraint(:linkage_event_id)
  end
end
