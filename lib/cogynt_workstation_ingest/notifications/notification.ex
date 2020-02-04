defmodule CogyntWorkstationIngest.Notifications.Notification do
  use Ecto.Schema
  import Ecto.Changeset

  alias CogyntWorkstationIngest.Notifications.NotificationSetting
  alias CogyntWorkstationIngest.Events.Event
  alias CogyntWorkstationIngest.System.SystemTag

  @primary_key {:id, :binary_id, autogenerate: true}
  @derive {Phoenix.Param, key: :id}

  schema "notifications" do
    belongs_to :tag, SystemTag,
      foreign_key: :tag_id,
      type: :binary_id

    belongs_to :event, Event, foreign_key: :event_id, type: Ecto.UUID

    belongs_to :notification_setting, NotificationSetting,
      foreign_key: :notification_setting_id,
      type: Ecto.UUID

    field :title, :string
    field :user_id, Ecto.UUID, allow_nil: true
    field :dismissed_at, :utc_datetime
    field :deleted_at, :utc_datetime

    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  @doc false
  def changeset(notification, attrs) do
    notification
    |> cast(attrs, [
      :title,
      :tag_id,
      :user_id,
      :event_id,
      :notification_setting_id,
      :deleted_at,
      :dismissed_at
    ])
    |> validate_required([
      :title,
      :event_id,
      :notification_setting_id,
      :tag_id
    ])
    |> foreign_key_constraint(:notification_setting_id)
    |> foreign_key_constraint(:event_id)
    |> foreign_key_constraint(:tag_id)
  end
end
