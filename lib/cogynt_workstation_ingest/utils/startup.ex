defmodule CogyntWorkstationIngest.Utils.Startup do
  @moduledoc """
  Module that is used for tasks that need to run upon Application startup
  """
  import Ecto.Query
  alias Models.Events.EventDefinition
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor
  alias CogyntWorkstationIngest.Repo

  # ------------------------- #
  # --- application calls --- #
  # ------------------------- #
  def initialize_consumers() do
    with :ok <- Application.ensure_started(:phoenix),
         :ok <- Application.ensure_started(:ecto_sql),
         :ok <- Application.ensure_started(:ecto_enum),
         :ok <- Application.ensure_started(:postgrex) do
      query =
        from(
          ed in EventDefinition,
          where: is_nil(ed.deleted_at),
          where: ed.active == true
        )

      Repo.transaction(fn ->
        Repo.stream(query)
        |> Stream.each(fn ed ->
          ed
          |> Repo.preload(:event_definition_details)
          |> convert_event_definition()
          |> ConsumerGroupSupervisor.start_child()
        end)
        |> Enum.to_list()
      end)
    else
      {:error, error} ->
        IO.puts("App not started. #{inspect(error)}")
    end
  end

  # ----------------------- #
  # --- private methods --- #
  # ----------------------- #
  defp convert_event_definition(event_definition) do
    event_definition_details =
      case event_definition do
        %{event_definition_details: %Ecto.Association.NotLoaded{}} ->
          []

        %{event_definition_details: details} ->
          details

        _ ->
          []
      end

    %{
      id: event_definition.id,
      title: event_definition.title,
      topic: event_definition.topic,
      event_type: event_definition.event_type,
      deleted_at: event_definition.deleted_at,
      authoring_event_definition_id: event_definition.authoring_event_definition_id,
      active: event_definition.active,
      created_at: event_definition.created_at,
      updated_at: event_definition.updated_at,
      primary_title_attribute: event_definition.primary_title_attribute,
      fields:
        Enum.reduce(event_definition_details, %{}, fn
          %{field_name: n, field_type: t}, acc ->
            Map.put_new(acc, n, t)

          _, acc ->
            acc
        end)
    }
  end
end
