defmodule CogyntWorkstationIngest.Notifications.NotificationSetting do
  use Ecto.Schema
  import Ecto.Changeset

  alias CogyntWorkstationIngest.Notifications.Notification
  alias CogyntWorkstationIngest.Events.EventDefinition
  alias CogyntWorkstationIngest.System.SystemTag

  @primary_key {:id, :binary_id, autogenerate: true}
  @derive {Phoenix.Param, key: :id}

  schema "notification_settings" do
    belongs_to :tag, SystemTag,
      foreign_key: :tag_id,
      type: :binary_id

    belongs_to :event_definition, EventDefinition,
      foreign_key: :event_definition_id,
      type: Ecto.UUID

    field :title, :string
    field :description, :string
    field :user_id, Ecto.UUID, allow_nil: true
    field :deleted_at, :utc_datetime
    field :active, :boolean, default: true
    field :role, :string, virtual: true

    has_many :notifications, Notification
    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  @doc false
  def changeset(notification_setting, %{role: :system} = attrs) do
    attrs = %{attrs | user_id: nil}

    notification_setting
    |> cast(attrs, [:title, :tag_id, :event_definition_id, :user_id, :deleted_at, :active])
    |> validate_required([:title, :tag_id, :event_definition_id])
    |> foreign_key_constraint(:event_definition_id)
    |> foreign_key_constraint(:tag_id)
  end

  def changeset(notification_setting, attrs) do
    notification_setting
    |> cast(attrs, [:title, :tag_id, :event_definition_id, :user_id, :deleted_at, :active])
    |> validate_required([:title, :tag_id, :event_definition_id])
    |> foreign_key_constraint(:event_definition_id)
    |> foreign_key_constraint(:tag_id)
  end
end
