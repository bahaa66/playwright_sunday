defmodule CogyntWorkstationIngest.Utils.JobQueue.Workers.DeleteEventDefinitionEventsWorker do
  @moduledoc """
  Worker module that will be called by the Exq Job Queue to execute the
  DeleteEventDefinitionEvents work
  """
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Broadway.EventPipeline
  alias CogyntWorkstationIngest.Events.EventsContext
  alias CogyntWorkstationIngest.Repo
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias Ecto.Multi

  alias Models.Events.EventDefinition
  alias Models.Enums.ConsumerStatusTypeEnum

  @page_size 2000

  def perform(event_definition_id), do: update_event_definition_events(event_definition_id)

  # ----------------------- #
  # --- Private Methods --- #
  # ----------------------- #
  defp update_event_definition_events(event_definition_id) do
    with %EventDefinition{} = event_definition <-
           EventsContext.get_event_definition(event_definition_id) do
      # First stop the consumer
      {_status, consumer_state} = ConsumerStateManager.get_consumer_state(event_definition.id)

      if consumer_state.status != ConsumerStatusTypeEnum.status()[:unknown] do
        Redis.publish_async("ingest_channel", %{
          stop_consumer: EventsContext.remove_event_definition_virtual_fields(event_definition)
        })

        ensure_event_pipeline_stopped(event_definition.id)
      end

      # Second soft delete_event_definition_event_detail_templates_data˝
      EventsContext.delete_event_definition_event_detail_templates_data(event_definition)

      # Third remove all documents from elasticsearch
      Elasticsearch.delete_by_query(Config.event_index_alias(), %{
        field: "event_definition_id",
        value: event_definition_id
      })

      case EventsContext.get_core_ids_for_event_definition_id(event_definition_id) do
        [] ->
          nil

        core_ids ->
          Elasticsearch.delete_by_query(Config.risk_history_index_alias(), %{
            field: "id",
            value: core_ids
          })
      end

      # Fourth paginate through all the events linked to the event_definition_id and
      # soft delete them
      process_events(event_definition.id)
      |> case do
        {:ok, result} ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "Finished removing all event data for event_definition_id #{event_definition_id}: #{
              inspect(result, pretty: true)
            }"
          )

          # Update event_definition to be inactive
          EventsContext.update_event_definition(event_definition, %{
            active: false,
            deleted_at: nil
          })

          # remove all state in Redis that is linked to event_definition_id
          ConsumerStateManager.remove_consumer_state(event_definition_id)
          Redis.publish_async("event_definitions_subscription", %{updated: event_definition_id})

        {:error, error} ->
          CogyntLogger.error(
            "#{__MODULE__}",
            "Error deleting event data for #{event_definition_id}. Error: #{
              inspect(error, pretty: true)
            }"
          )
      end
    else
      nil ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Event definition not found for EventDefinitionI: #{event_definition_id}"
        )
    end
  end

  defp process_events(event_definition_id, accumulator \\ %{})
       when is_binary(event_definition_id) do
    Multi.new()
    |> Multi.run(:create_temp_view, fn _, _ ->
      view_name = "delete_events_#{String.replace(event_definition_id, "-", "_")}"

      Ecto.Adapters.SQL.query(Repo, """
        CREATE OR REPLACE TEMPORARY VIEW #{view_name} AS
          SELECT id
          FROM events
          WHERE event_definition_id='#{event_definition_id}'
          AND deleted_at IS NULL
          LIMIT #{@page_size};
      """)
      |> case do
        {:ok, _} ->
          {:ok, view_name}

        {:error, error} ->
          {:error, error}
      end
    end)
    |> Multi.run(:delete_event_links, fn _repo, %{create_temp_view: view_name} ->
      Ecto.Adapters.SQL.query(Repo, """
        UPDATE event_links el
        SET deleted_at=now()
        FROM #{view_name} d
        WHERE d.id = el.linkage_event_id;
      """)
    end)
    |> Multi.run(:delete_events, fn _repo, %{create_temp_view: view_name} ->
      Ecto.Adapters.SQL.query(Repo, """
        UPDATE events e
        SET deleted_at=now()
        FROM #{view_name} d
        WHERE d.id = e.id;
      """)
    end)
    |> Repo.transaction()
    |> case do
      {:ok,
       %{
         delete_events: %Postgrex.Result{num_rows: num_rows},
         create_temp_view: view_name
       } = r}
      when num_rows < @page_size ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Finished deleting events for #{event_definition_id}."
        )

        Ecto.Adapters.SQL.query(Repo, "DROP VIEW IF EXISTS #{view_name}")
        {:ok, build_accumulator(r, accumulator)}

      {:ok, result} ->
        accumulator = build_accumulator(result, accumulator)
        process_events(event_definition_id, accumulator)

      {:error, :create_temp_view, e, _} ->
        {:error, e}

      {:error, _, e, %{create_temp_view: view_name}} ->
        Ecto.Adapters.SQL.query(Repo, "DROP VIEW IF EXISTS #{view_name}")
        {:error, e}
    end
  end

  defp build_accumulator(result, accumulator) do
    Enum.reduce(result, accumulator, fn
      {:delete_events, %Postgrex.Result{num_rows: num_rows}}, a ->
        total_events = Map.get(a, :total_events, 0)
        Map.put(a, :total_events, total_events + num_rows)

      {:delete_event_links, %Postgrex.Result{num_rows: num_rows}}, a ->
        total_event_links = Map.get(a, :total_event_links, 0)
        Map.put(a, :total_event_links, total_event_links + num_rows)

      _, a ->
        a
    end)
  end

  defp ensure_event_pipeline_stopped(event_definition_id, count \\ 1) do
    if count >= 30 do
      CogyntLogger.info(
        "#{__MODULE__}",
        "ensure_event_pipeline_stopped/1 exceeded number of attempts. Moving forward with DeleteEventDefinitionEvents"
      )
    else
      case EventPipeline.event_pipeline_running?(event_definition_id) or
             not EventPipeline.event_pipeline_finished_processing?(event_definition_id) do
        true ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} still running... waiting for it to shutdown before resetting data"
          )

          Process.sleep(500)
          ensure_event_pipeline_stopped(event_definition_id, count + 1)

        false ->
          CogyntLogger.info(
            "#{__MODULE__}",
            "EventPipeline #{event_definition_id} Stopped"
          )
      end
    end
  end
end
