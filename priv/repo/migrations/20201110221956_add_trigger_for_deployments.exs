defmodule CogyntWorkstationIngest.Repo.Migrations.AddTriggerForDeployments do
  use Ecto.Migration

  @table_name "deployments"
  @function_name "notify_deployment_changes"
  @trigger_name "deployments_changed"

  def change do
    execute("DROP TRIGGER IF EXISTS #{@trigger_name} ON #{@table_name}")

    execute("""
      CREATE TRIGGER #{@trigger_name}
      AFTER INSERT OR UPDATE OR DELETE
      ON #{@table_name}
      FOR EACH ROW
      EXECUTE PROCEDURE #{@function_name}()
    """)
  end

  def down do
    execute("DROP TRIGGER IF EXISTS #{@trigger_name} ON #{@table_name}")
  end
end
