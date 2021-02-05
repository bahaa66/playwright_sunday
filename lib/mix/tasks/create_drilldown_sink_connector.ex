defmodule Mix.Tasks.CreateDrilldownSinkConnector do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session and run commands to manually create the
  drilldown sink connector
  """
  use Mix.Task
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Drilldown.DrilldownSinkConnector

  @impl Mix.Task
  def run(_) do
    if Config.session_domain() != "localhost" do
      with {:ok, _} <- HTTPoison.start(),
           {:ok, _} <- DrilldownSinkConnector.kafka_connect_health(),
           {:ok, _} <- DrilldownSinkConnector.create_or_update() do
        Mix.shell().info(
          "Drilldown Sink connector #{Config.ts_connector_name()} and #{
            Config.tse_connector_name()
          } has been successfully created."
        )
      else
        {:error, error} ->
          CogyntLogger.error(
            __MODULE__,
            "Create Drilldown Sink Connector failed: An error occured trying to create the connector #{
              Config.ts_connector_name()
            }
            and #{Config.tse_connector_name()}. Error: #{inspect(error)}"
          )

          Mix.raise("""
            An error occured trying to create the connector #{Config.ts_connector_name()} and #{
            Config.tse_connector_name()
          }. Error: #{inspect(error)}
          """)

        _ ->
          Mix.raise("""
            An unexpected error occurred trying to create Drilldown Sink Connector.
          """)
      end
    end
  end
end
