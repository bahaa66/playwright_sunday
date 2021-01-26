defmodule CogyntWorkstationIngest.Repo.Migrations.CreateIndexTemplateSolution do
  use Ecto.Migration

  def change do
    create_if_not_exists index("template_solution", [:id])
  end
end
