defmodule CogyntWorkstationIngestWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :cogynt_workstation_ingest

  socket("/ws/ingest", CogyntWorkstationIngestWeb.Channels.IngestSocket,
    websocket: true,
    longpoll: false
  )

  socket("/live", Phoenix.LiveView.Socket)

  # Serve at "/" the static files from "priv/static" directory.
  #
  # You should set gzip to true if you are running phx.digest
  # when deploying your static files in production.
  plug(Plug.Static,
    at: "/",
    from: :cogynt_workstation_ingest,
    gzip: false,
    only: ~w(css fonts images js favicon.ico robots.txt)
  )

  # Code reloading can be explicitly enabled under the
  # :code_reloader configuration of your endpoint.
  if code_reloading? do
    plug(Phoenix.CodeReloader)
  end

  plug(Phoenix.LiveDashboard.RequestLogger,
    param_key: "request_logger",
    cookie_key: "request_logger"
  )

  plug(Plug.RequestId)
  # plug(Plug.Telemetry, event_prefix: [:phoenix, :endpoint])
  plug(Plug.QuietLogger, path: ["/healthz", "/livenessCheck", "/rpc/ingest", "/dashboard"])

  plug(Plug.Parsers,
    parsers: [:urlencoded, :multipart, :json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()
  )

  plug(Plug.MethodOverride)
  plug(Plug.Head)

  # The session will be stored in the cookie and signed,
  # this means its contents can be read but not tampered with.
  # Set :encryption_salt if you would also like to encrypt it.
  plug(AuthSession,
    log: :debug,
    store: :cookie
  )

  plug(CogyntWorkstationIngestWeb.Router)
end
