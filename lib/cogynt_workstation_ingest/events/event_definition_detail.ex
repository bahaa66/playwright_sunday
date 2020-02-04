defmodule CogyntWorkstationIngest.Events.EventDefinitionDetail do
  use Ecto.Schema
  import Ecto.Changeset
  alias CogyntWorkstationIngest.Events.EventDefinition

  @foreign_key_type Ecto.UUID

  schema "event_definition_details" do
    field :field_name, :string, null: false
    field :field_type, :string

    belongs_to :event_definition, EventDefinition,
      foreign_key: :event_definition_id,
      type: Ecto.UUID
  end

  def changeset(details, attr) do
    details
    |> cast(attr, [:event_definition_id, :field_name, :field_type])
    |> validate_required([:event_definition_id, :field_name, :field_type])
    |> foreign_key_constraint(:event_definition_id)
  end
end
