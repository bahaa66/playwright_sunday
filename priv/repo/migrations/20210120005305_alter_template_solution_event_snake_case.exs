defmodule CogyntWorkstationIngest.Repo.Migrations.AlterTemplateSolutionEventSnakeCase do
  use Ecto.Migration

  def change do
    rename table(:template_solution_events), :templateTypeName, to: :template_type_name
    rename table(:template_solution_events), :templateTypeId, to: :template_type_id
    rename table(:template_solution_events), :assertionName, to: :assertion_name
  end
end
