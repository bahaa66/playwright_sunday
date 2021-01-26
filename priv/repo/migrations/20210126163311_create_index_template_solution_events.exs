defmodule CogyntWorkstationIngest.Repo.Migrations.CreateIndexTemplateSolutionEvents do
  use Ecto.Migration

  def change do
    create_if_not_exists index("template_solution_events", [:id])
  end
end
