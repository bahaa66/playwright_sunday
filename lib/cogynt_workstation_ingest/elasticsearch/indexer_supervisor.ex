defmodule CogyntWorkstationIngest.Elasticsearch.IndexerSupervisor do
  # CURRENTLY NOT USED UNTIL WE START USING LIBCLUSTER AND HORDE REGISTRY AGAIN
  # NEED TO FIGURE OUT WHY THEY DONT WORK WITH ISTIO
  use Horde.DynamicSupervisor

  def start_link(_) do
    Horde.DynamicSupervisor.start_link(__MODULE__, [strategy: :one_for_one], name: __MODULE__)
  end

  def init(init_arg) do
    [members: members()]
    |> Keyword.merge(init_arg)
    |> Horde.DynamicSupervisor.init()
  end

  def start_child(child_spec) do
    Horde.DynamicSupervisor.start_child(__MODULE__, child_spec)
  end

  def members() do
    Enum.map([Node.self() | Node.list()], &{__MODULE__, &1})
  end
end
