defmodule CogyntWorkstationIngest.Repo.Migrations.CreateExportBuilders do
  use Ecto.Migration

  def change do
    create_if_not_exists table(:export_builders, primary_key: false) do
      add(:id, :uuid, primary_key: true)
      add(:name, :string, null: false)
      add(:object_type, :string, null: false)
      add(:export_schema, :text, null: false)
      add(:field_mappings, :text, null: false)
      add(:deleted_at, :utc_datetime)
      timestamps(inserted_at: :created_at, type: :utc_datetime)
    end
  end
end
