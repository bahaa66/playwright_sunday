defmodule AuthSession do
  alias CogyntWorkstationIngest.Config
  def init(opts), do: opts

  def call(conn, opts) do
    opts =
      opts
      |> Keyword.put(:domain, "#{Config.session_domain()}")
      |> Keyword.put(:key, Config.session_key())
      |> Keyword.put(:signing_salt, Config.signing_salt())

    runtime_opts = Plug.Session.init(opts)
    Plug.Session.call(conn, runtime_opts)
  end
end
