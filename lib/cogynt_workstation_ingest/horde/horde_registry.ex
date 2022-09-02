defmodule CogyntWorkstationIngest.Horde.HordeRegistry do
  # CURRENTLY NOT USED UNTIL WE START USING LIBCLUSTER AND HORDE REGISTRY AGAIN
  # NEED TO FIGURE OUT WHY THEY DONT WORK WITH ISTIO
  use Horde.Registry

  def start_link(_) do
    Horde.Registry.start_link(__MODULE__, [keys: :unique], name: __MODULE__)
  end

  def init(init_arg) do
    [members: members()]
    |> Keyword.merge(init_arg)
    |> Horde.Registry.init()
  end

  defp members() do
    Enum.map([Node.self() | Node.list()], &{__MODULE__, &1})
  end
end
