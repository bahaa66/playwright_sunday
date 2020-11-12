defmodule CogyntWorkstationIngest.Repo.Migrations.AddNotificationFunctionForDeployments do
  use Ecto.Migration

  @function_name "notify_deployment_changes"
  @event_name "deployments_changed"

  def up do
    execute("""
      CREATE OR REPLACE FUNCTION #{@function_name}()
      RETURNS trigger AS $$
      BEGIN
        PERFORM pg_notify(
          '#{@event_name}',
          json_build_object(
            'operation', TG_OP,
            'record_id', CASE TG_OP WHEN 'INSERT' THEN NEW.id
                                    WHEN 'UPDATE' THEN NEW.id
                                    WHEN 'DELETE' THEN OLD.id
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
