defmodule Mix.Tasks.FlushRedisDb do
  @moduledoc """
  Running this task will alleviate the need to start an `iex` session to manually flush the
  Redis data when testing out a new client deployment.
  """
  use Mix.Task

  @impl Mix.Task
  def run(_) do
    {:ok, conn} = Redix.start_link("redis://localhost:6379", name: :redix)
    Redis.flush_db()
    Redix.stop(conn)
  end
end
