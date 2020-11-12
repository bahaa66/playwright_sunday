defmodule CogyntWorkstationIngest.Repo.Migrations.AddTriggerForSystemNotifications do
  use Ecto.Migration

  @table_name "system_notifications"
  @function_name "system_notification_changes"
  @trigger_name "system_notifications_changed"

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
