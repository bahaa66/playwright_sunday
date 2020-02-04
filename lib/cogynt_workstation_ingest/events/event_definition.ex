import EctoEnum
defenum(EventTypeEnum, :event_type, [:entity, :linkage, :none])

defmodule CogyntWorkstationIngest.Events.EventDefinition do
  use Ecto.Schema
  import Ecto.Changeset
  alias CogyntWorkstationIngest.Events.{EventDefinitionDetail, Event}
  alias CogyntWorkstationIngest.Notifications.NotificationSetting

  @primary_key {:id, :binary_id, []}
  @derive {Phoenix.Param, key: :id}

  schema "event_definitions" do
    field :topic, :string
    field :title, :string
    field :event_type, EventTypeEnum, default: :none
    field :deleted_at, :utc_datetime, allow_nil: true, default: nil
    field :authoring_event_definition_id, Ecto.UUID
    field :active, :boolean, allow_nil: false, default: true
    field :primary_title_attribute, :string

    has_many :events, Event
    has_many :event_definition_details, EventDefinitionDetail

    has_many :notification_settings, NotificationSetting, on_delete: :delete_all

    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  def changeset(definition, attrs) do
    definition
    |> cast(attrs, [
      :topic,
      :title,
      :id,
      :event_type,
      :deleted_at,
      :authoring_event_definition_id,
      :active,
      :primary_title_attribute
    ])
    |> unique_constraint(:id)
    |> unique_constraint(:unique_active_event_definition,
      name: :unique_active_event_definition_index,
      message:
        "an event_definition with that authoring_event_definition_id already exists with that deleted_at value"
    )
    |> validate_required([:topic, :title, :id, :authoring_event_definition_id, :active])
  end
end
