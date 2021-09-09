defmodule CogyntWorkstationIngest.Servers.Workers.TestWorker do
  use GenServer
  require Logger

  alias __MODULE__.Runner

  @impl GenServer
  def init(args \\ []) do
    timeout = Keyword.get(args, :timeout)
    schedule(timeout)
    {:ok, timeout}
  end

  @impl GenServer
  def handle_info(:execute, timeout) do
    Task.start(
      Runner,
      :execute,
      []
    )

    schedule(timeout)

    {:noreply, timeout}
  end

  defp schedule(timeout) do
    Process.send_after(self(), :execute, timeout)
  end
end
