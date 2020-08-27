defmodule CogyntWorkstationIngest.Broadway.DrilldownProcessor do
  @moduledoc """
  Module that acts as the Broadway Processor for the DrilldownPipeline.
  """

  alias CogyntWorkstationIngest.Drilldown.DrilldownContext
  alias Broadway.Message

  @doc """
  process_template_data/1
  """
  def process_template_data(%Message{data: nil}) do
    raise "process_template_data/1 failed. No message data"
  end

  def process_template_data(%Message{data: %{event: event_message} = data} = message) do
    case event_message["event"] do
      nil ->
        solution = %{
          "retracted" => event_message["retracted"],
          "template_type_name" => event_message["template_type_name"],
          "template_type_id" => event_message["template_type_id"],
          "id" => event_message["id"]
        }

        data =
          Map.put(data, :solution_id, solution["id"])
          |> Map.put(:solution, solution)

        Map.put(message, :data, data)

      value ->
        event =
          value
          |> Map.put("assertion_id", event_message["aid"])

        solution = %{
          "id" => event_message["id"]
        }

        data =
          Map.put(data, :solution_id, solution["id"])
          |> Map.put(:solution, solution)
          |> Map.put(:solution_event, event)

        Map.put(message, :data, data)
    end
  end

  @doc """
  upsert_template_solutions/1 passes the data map object to the DrilldownCache to
  have its state updated with the new data
  """
  def upsert_template_solutions(%Message{data: nil}) do
    raise "upsert_template_solutions/1 failed. No message data"
  end

  def upsert_template_solutions(%Message{data: data} = message) do
    DrilldownContext.upsert_template_solutions(data)
    message
  end
end
