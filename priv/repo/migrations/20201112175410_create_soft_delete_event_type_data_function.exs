defmodule CogyntWorkstationIngest.Repo.Migrations.CreateSoftDeleteEventTypeDataProcedure do
  use Ecto.Migration

  @events_table_name "events"
  @event_detail_templates_table_name "event_detail_templates"
  @event_detail_template_groups_table_name "event_detail_template_groups"
  @event_detail_template_group_items_table_name "event_detail_template_group_items"
  @event_links_table_name "event_links"
  @proc_name "soft_delete_event_type_data"

  def up do
    execute("""
    CREATE PROCEDURE #{@proc_name} @EventDefinitionId uuid
    AS

    BEGIN
      DECLARE @PageNumber AS INT
      DECLARE @RowsOfPage AS INT
      DECLARE @SortingCol AS VARCHAR(100) ='created_at'
      DECLARE @SortType AS VARCHAR(100) = 'DESC'
      SET @PageNumber=1
      SET @RowsOfPage=2000
      SELECT id FROM #{@events_table_name} WHERE event_definition_id = @EventDefinitionId AND deleted_at IS NULL
      ORDER BY
      CASE WHEN @SortingCol = 'created_at' AND @SortType ='DESC' THEN created_at END DESC,
      OFFSET (@PageNumber-1)*@RowsOfPage ROWS
      FETCH NEXT @RowsOfPage ROWS ONLY
    END;


    BEGIN
    update events
    END;

    BEGIN
    update event_links
    END;

    GO;
    $$  LANGUAGE plpgsql;
    """)
  end

  def down do
    execute("DROP FUNCTION IF EXISTS #{@function_name} CASCADE")
  end
end
