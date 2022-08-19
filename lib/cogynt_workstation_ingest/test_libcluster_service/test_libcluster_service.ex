defmodule CogyntWorkstationIngest.TestLibclusterService do
  use GenServer
  require Logger

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl GenServer
  def init(args) do
    display_message(args)
    Process.send_after(self(), :display_message, 15_000)
    {:ok, args}
  end

  @impl GenServer
  def handle_info(:display_message, state) do
    display_message(state)
    Process.send_after(self(), :display_message, 15_000)
    {:noreply, state}
  end

  def display_message(state) do
    Logger.info("RUNNING ON #{inspect(Node.self())} with name #{inspect(state)}")
    Logger.info("\n\n########################################\n#                                      #\n#    COGYNT TEST LIBCLUSTER SERVICE    #\n#                                      #\n########################################\n\n")
  end
end
