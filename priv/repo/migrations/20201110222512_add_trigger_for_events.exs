defmodule CogyntWorkstationIngest.Repo.Migrations.AddTriggerForEvents do
  use Ecto.Migration

  @table_name "events"
  @function_name "notify_event_changes"
  @trigger_name "events_changed"

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
