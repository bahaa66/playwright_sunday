defmodule CogyntWorkstationIngest.TestLibclusterService.Starter do
  alias CogyntWorkstationIngest.TestLibclusterService, as: TestService
  alias CogyntWorkstationIngest.Horde.HordeRegistry
  alias CogyntWorkstationIngest.TestLibclusterService.Supervisor, as: TestSupervisor

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

  def start_link(opts) do
    name =
      opts
      |> Keyword.get(:name, TestService)
      |> via_tuple()

    new_opts = Keyword.put(opts, :name, name)

    child_spec = %{
      id: TestService,
      start: {TestService, :start_link, [new_opts]}
    }

    TestSupervisor.start_child(child_spec)
    |> case do
      {:error, {:already_started, pid}} -> {:ok, pid}
      res -> res
    end
  end

  def whereis(name \\ TestService) do
    name
    |> via_tuple()
    |> GenServer.whereis()
  end

  defp via_tuple(name) do
    {:via, Horde.Registry, {HordeRegistry, name}}
  end
end
