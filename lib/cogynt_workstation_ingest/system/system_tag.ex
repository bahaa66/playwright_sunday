defmodule CogyntWorkstationIngest.System.SystemTag do
  @moduledoc """
  Module that defines the Ecto Schema and generates the changesets for System Tags.
  """
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @derive {Phoenix.Param, key: :id}

  schema "system_tags" do
    field :name, :string, allow_nil: false
    field :color, :string, allow_nil: false
    field :order, :integer, allow_nil: false

    timestamps(inserted_at: :created_at, type: :utc_datetime)
  end

  def changeset(system_tag, attrs) do
    system_tag
    |> cast(attrs, [
      :name,
      :color,
      :order
    ])
    |> unique_constraint(
      :unique_tag_name,
      name: :unique_tag_name_index,
      message: "A tag with this name already exists"
    )
    |> unique_constraint(
      :unique_tag_color,
      name: :unique_tag_color_index,
      message: "A tag with this color already exists"
    )
    |> validate_required([:name, :color, :order])
  end
end
