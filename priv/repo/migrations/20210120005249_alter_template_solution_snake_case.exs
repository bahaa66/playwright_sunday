defmodule CogyntWorkstationIngest.Repo.Migrations.AlterTemplateSolutionSnakeCase do
  use Ecto.Migration

  def change do
    rename table(:template_solution), :templateTypeId, to: :template_type_id
    rename table(:template_solution), :templateTypeName, to: :template_type_name
  end
end
