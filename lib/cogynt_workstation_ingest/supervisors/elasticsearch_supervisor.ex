defmodule CogyntWorkstationIngest.Supervisors.Elasticsearch do
  @moduledoc """
  Supervisor to start elasticsearch cluster supervisor
  """
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    children = [
      {CogyntWorkstationIngest.Elasticsearch.Cluster, []}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
