defmodule CogyntWorkstationIngest.Utils.Tasks.DeleteEventDefinitionsAndTopicsTask do
  @moduledoc """
  Task module that can bee called to execute the delete_topic_data work as a
  async task.
  """
  use Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Notifications.NotificationsContext
  alias CogyntWorkstationIngest.Collections.CollectionsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext

  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum
  alias Models.Notifications.NotificationSetting

  @page_size 2000

  def start_link(arg) do
    Task.start_link(__MODULE__, :run, [arg])
  end

  def run(%{
        event_definition_ids: event_definition_ids,
        hard_delete: hard_delete_event_definitions,
        delete_topics: delete_topics
      }) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Running delete_topic_data_task for event_definition_ids: #{event_definition_ids}, hard_delete_event_definitions: #{
        hard_delete_event_definitions
      }, delete_topics: #{delete_topics}"
    )

    Enum.each(event_definition_ids, fn event_definition_id ->
      case EventsContext.get_event_definition(event_definition_id) do
        nil ->
          CogyntLogger.warn(
            "#{__MODULE__}",
            "Event definition not found for event_definition_id: #{event_definition_id}"
          )

        event_definition ->
          # First stop the EventPipeline if there is one running for the event_definition
          stop_event_pipeline(event_definition)

          # Second check to see if the topic needs to be deleted
          if delete_topics do
            delete_topics(event_definition)
          end

          # Third remove all records from Elasticsearch
          delete_elasticsearch_data(event_definition)

          if not hard_delete_event_definitions do
            delete_event_definition(event_definition)
          else
            ConsumerStateManager.remove_consumer_state(event_definition.id,
              hard_delete_event_definition: true
            )
          end
      end
    end)

    if hard_delete_event_definitions do
      truncate_all_tables()

      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished deleting data for EventDefinitionIds: #{inspect(event_definition_ids)}"
      )
    end
  end

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp stop_event_pipeline(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Stopping EventPipeline for #{event_definition.topic}"
    )

    {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

    # Remove any pending tasks that may be in the queue
    ConsumerStateManager.upsert_consumer_state(event_definition.id,
      backfill_notifications: [],
      update_notifications: [],
      delete_notifications: []
    )

    if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
      Redis.publish_async("ingest_channel", %{
        stop_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
      })
    end

    ensure_event_pipeline_stopped(event_definition.id)
  end

  defp delete_topics(event_definition) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting Kakfa topic: #{event_definition.topic}"
    )

    case DeploymentsContext.get_kafka_brokers(event_definition.deployment_id) do
      {:ok, brokers} ->
        Kafka.Api.Topic.delete_topic(event_definition.topic, brokers)

      {:error, :does_not_exist} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to fetch brokers for DeploymentId: #{event_definition.deployment_id}"
        )
    end
  end

  defp delete_event_definition(event_definition) do
    # delete data
    CogyntLogger.info(
      "#{__MODULE__}",
      "Deleting data for EventDefinitionId: #{event_definition.id}"
    )

    # Sixth delete all event_definition_data. Anything linked to the
    # event_definition_id
    page =
      EventsContext.get_page_of_events(
        %{
          filter: %{event_definition_id: event_definition.id},
          select: [:id]
        },
        page_number: 1,
        page_size: @page_size,
        include_deleted: true
      )

    if length(page.entries) > 0 do
      delete_event_definition_data(page, event_definition.id)
    end

    # Finally update the EventDefintion to be inactive
    case EventsContext.update_event_definition(event_definition, %{
           active: false
         }) do
      {:ok, %EventDefinition{} = updated_event_definition} ->
        ConsumerStateManager.remove_consumer_state(event_definition.id)

        Redis.publish_async(
          "event_definitions_subscription",
          %{updated: updated_event_definition.id}
        )

        CogyntLogger.info(
          "#{__MODULE__}",
          "Finished deleting data for event definition: #{updated_event_definition.id}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Something went wrong deleting the event definition: #{inspect(error)}"
        )
    end
  end

  defp delete_elasticsearch_data(event_definition) do
    case Elasticsearch.delete_by_query(Config.event_index_alias(), %{
           field: "event_definition_id",
           value: event_definition.id
         }) do
      {:ok, _count} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Elasticsearch data deleted for EventDefinitionId: #{event_definition.id}"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "There was an error deleting elasticsearch data for event definition: #{
            event_definition.id
          }\nError: #{inspect(error)}"
        )
    end

    case EventsContext.get_core_ids_for_event_definition_id(event_definition.id) do
      [] ->
        nil

      core_ids ->
        Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
          field: "id",
          value: core_ids
        })
    end
  end

  defp delete_event_definition_data(
         %{entries: events, page_number: page_number, total_pages: total_pages},
         event_definition_id
       ) do
    event_ids = Enum.map(events, fn e -> e.id end)

    # Delete notifications
    {_notification_count, notifications} =
      NotificationsContext.hard_delete_notifications(%{
        filter: %{event_ids: event_ids},
        select: [:id]
      })

    notification_ids = Enum.map(notifications, fn n -> n.id end)

    # Delete notification_settings
    {_notification_settings_count, _notification_settings} =
      NotificationsContext.hard_delete_notification_settings(%{
        filter: %{
          event_definition_id: event_definition_id
        },
        select: NotificationSetting.__schema__(:fields)
      })

    # Delete notification collection items
    {_notification_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        filter: %{
          item_ids: notification_ids,
          item_type: "notification"
        }
      })

    # Delete event details
    {_event_detail_count, _} =
      EventsContext.hard_delete_event_details(%{
        filter: %{event_ids: event_ids}
      })

    # Delete events
    {_event_count, _} =
      EventsContext.hard_delete_events(%{
        filter: %{event_ids: event_ids}
      })

    # TODO
    # Delete event links

    # TODO
    # Delete event definition details

    # TODO
    # Delete event detail template group items

    # TODO
    # Delete event detail template groups

    # TODO
    # Delete event detail templates

    # Delete event collection items
    {_event_item_count, _} =
      CollectionsContext.hard_delete_collection_items(%{
        filter: %{
          item_ids: event_ids,
          item_type: "event"
        }
      })

    CogyntLogger.info(
      "#{__MODULE__}",
      "Removing Events and Associated Data for PageNumber: #{page_number} out of TotalPages: #{
        total_pages
      }"
    )

    if page_number >= total_pages do
      CogyntLogger.info(
        "#{__MODULE__}",
        "Finished Removing All EventDefinitionData For DevDelete Action"
      )
    else
      next_page =
        EventsContext.get_page_of_events(
          %{
            filter: %{event_definition_id: event_definition_id},
            select: [:id]
          },
          page_number: page_number + 1,
          page_size: @page_size,
          include_deleted: true
        )

      delete_event_definition_data(next_page, event_definition_id)
    end
  end

  defp ensure_event_pipeline_stopped(event_definition_id) do
    case EventPipeline.event_pipeline_running?(event_definition_id) or
           not EventPipeline.event_pipeline_finished_processing?(event_definition_id) do
      true ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before resetting data"
        )

        Process.sleep(500)
        ensure_event_pipeline_stopped(event_definition_id)

      false ->
        CogyntLogger.info("#{__MODULE__}", "EventPipeline #{event_definition_id} stopped")
    end
  end

  def truncate_all_tables() do
    try do
      {:ok, result = %Postgrex.Result{}} =
        Repo.query(
          "SELECT truncate_tables('#{Config.postgres_username()}')",
          []
        )

      CogyntLogger.info(
        "#{__MODULE__}",
        "truncate_all_tables completed with result: #{result.connection_id}"
      )
    rescue
      _ ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "truncate_all_tables failed"
        )
    end
  end
end
