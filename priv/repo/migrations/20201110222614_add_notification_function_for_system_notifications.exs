defmodule CogyntWorkstationIngest.Repo.Migrations.AddNotificationFunctionForSystemNotifications do
  use Ecto.Migration

  @function_name "system_notification_changes"
  @event_name "system_notifications_changed"

  def up do
    execute("""
      CREATE OR REPLACE FUNCTION #{@function_name}()
      RETURNS trigger AS $$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          PERFORM pg_notify(
            '#{@event_name}',
            json_build_object(
              'operation', TG_OP,
              'record', NEW
            )::text
          );
        ELSEIF TG_OP = 'UPDATE' THEN
          PERFORM pg_notify(
            '#{@event_name}',
            json_build_object(
              'operation', TG_OP,
              'record', NEW,
              'old_record', OLD
            )::text
          );
        ELSE
          PERFORM pg_notify(
            '#{@event_name}',
            json_build_object(
              'operation', TG_OP,
              'old_record', OLD
            )::text
          );
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    """)
  end

  def down do
    execute("DROP FUNCTION IF EXISTS #{@function_name} CASCADE")
  end
end
