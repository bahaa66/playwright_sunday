defmodule CogyntWorkstationIngest.Repo.Migrations.AddNotificationFunctionForEventDefinitions do
  use Ecto.Migration

  @function_name "notify_event_definition_changes"
  @event_name "event_definitions_changed"

  def up do
    execute("""
      CREATE OR REPLACE FUNCTION #{@function_name}()
      RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify(
          '#{@event_name}',
          json_build_object(
            'operation', TG_OP,
            'record', CASE TG_OP WHEN 'INSERT' THEN NEW
                                 WHEN 'UPDATE' THEN NEW
                                 WHEN 'DELETE' THEN OLD
                                 ELSE NULL
                      END
          )::text
        );
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    """)
  end

  def down do
    execute("DROP FUNCTION IF EXISTS #{@function_name} CASCADE")
  end
end
