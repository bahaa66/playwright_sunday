defmodule CogyntWorkstationIngestWeb.Schema.Types.DevDelete do
  use Absinthe.Schema.Notation
  alias CogyntGraphql.Middleware.Permissions
  alias CogyntWorkstationIngestWeb.Resolvers.DevDelete, as: DevDeleteResolver

  # ----------------- #
  # --- mutations --- #
  # ----------------- #
  object :dev_delete_mutations do
    @desc """
      Deletes most data in workstation including:
        - All PG data, except for Collections, Tag, Notes for Collections, and Attachments for Notes
        - Redis data
        - Elasticsearch data
        - Druid data
    """
    field :delete_all_data, non_null(:delete_data_response) do
      arg(:delete_deployment_topic, :boolean, default_value: false)
      middleware(Permissions, resources: [%{resource: "workstation.ingest", action: "write"}])
      resolve(&DevDeleteResolver.delete_data/3)
    end

    @desc "Removes drillown data from druid."
    field :reset_drilldown_data, non_null(:delete_data_response) do
      middleware(Permissions, resources: [%{resource: "workstation.ingest", action: "write"}])
      resolve(&DevDeleteResolver.reset_drilldown_data/3)
    end

    @desc "Deletes all data associated with the event definitions belonging to the list of ids passed."
    field :delete_event_definitions, non_null(:delete_data_response) do
      arg(:ids, non_null(list_of(non_null(:id))))
      middleware(Permissions, resources: [%{resource: "workstation.ingest", action: "write"}])
      resolve(&DevDeleteResolver.delete_event_definitions/3)
    end
  end

  object :delete_data_response do
    field(:status, non_null(:string))
  end
end
