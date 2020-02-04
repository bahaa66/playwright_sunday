defmodule CogyntWorkstationIngest.Events.EventDetail do
  use Ecto.Schema
  import Ecto.Changeset
  alias CogyntWorkstationIngest.Events.Event

  @foreign_key_type Ecto.UUID
  schema "event_details" do
    field :field_name, :string, null: false
    field :field_value, :string
    field :field_type, :string

    belongs_to :event, Event, foreign_key: :event_id, type: Ecto.UUID
  end

  def changeset(details, attr) do
    details
    |> cast(attr, [:field_name, :field_value, :field_type, :event_id])
    |> validate_required([:field_name, :field_value, :event_id])
    |> foreign_key_constraint(:event_id)
  end
end
