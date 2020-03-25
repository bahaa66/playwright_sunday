defmodule AuthSession do
  def init(opts), do: opts

  def call(conn, opts) do
    opts =
      opts
      |> Keyword.put(:domain, "#{Application.get_env(:cogynt_workstation_ingest, :session_domain)}")
      |> Keyword.put(:key, Application.get_env(:cogynt_workstation_ingest, :session_key))
      |> Keyword.put(:signing_salt, Application.get_env(:cogynt_workstation_ingest, :signing_salt))

    runtime_opts = Plug.Session.init(opts)
    Plug.Session.call(conn, runtime_opts)
  end
end
