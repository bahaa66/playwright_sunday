defmodule CogyntWorkstationIngest.Repo.Migrations.CreateTruncateAllTablesFunction do
  use Ecto.Migration

  @function_name "truncate_tables"

  def change do
    execute("""
    CREATE OR REPLACE FUNCTION #{@function_name}(username IN VARCHAR) RETURNS void AS $$
    DECLARE
    statements CURSOR FOR
        SELECT tablename FROM pg_tables
        WHERE tableowner = username AND schemaname = 'public' AND tablename IN (
          'collection_items',
          'event_definition_details',
          'event_detail_template_group_items',
          'event_detail_template_groups',
          'event_detail_templates',
          'event_details',
          'event_links',
          'notification_system_tags',
          'notifications',
          'notification_settings',
          'events',
          'event_definitions'
        );
    BEGIN
      FOR stmt IN statements LOOP
        EXECUTE 'TRUNCATE TABLE ' || quote_ident(stmt.tablename) || ' CASCADE;';
      END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    """)

  end
end
