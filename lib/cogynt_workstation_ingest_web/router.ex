defmodule CogyntWorkstationIngestWeb.Router do
  @dialyzer {:no_return, __checks__: 0}
  use CogyntWorkstationIngestWeb, :router
  import Phoenix.LiveView.Router

  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.TelemetrySupervisor
  alias CogyntWorkstationIngestWeb.FallbackController
  alias CogyntWorkstationIngestWeb.Views.LayoutView

  pipeline :api do
    plug(:accepts, ["json"])
    plug(:fetch_session)

    plug(Plug.Parsers,
      parsers: [:urlencoded, :multipart, :json, Absinthe.Plug.Parser],
      json_decoder: Jason
    )

    plug(CogyntWorkstationIngestWeb.Plugs.Context)
  end

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:put_root_layout, {LayoutView, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  ## Health Check route
  forward("/healthz", HealthCheckup)
  ## Liveness Check route
  forward("/livenessCheck", LivenessCheck)

  if Config.enable_dev_tools?() do
    import Phoenix.LiveDashboard.Router

    scope "/ingest" do
      pipe_through(:browser)

      live_dashboard("/dashboard",
        metrics: TelemetrySupervisor,
        additional_pages: [
          broadway: BroadwayDashboard
        ]
      )
    end
  end

  scope "/ingest/api" do
    pipe_through(:api)

    forward("/graphql", Absinthe.Plug, schema: CogyntWorkstationIngestWeb.Schema)

    if Config.enable_dev_tools?() do
      forward("/graphiql", Absinthe.Plug.GraphiQL,
        schema: CogyntWorkstationIngestWeb.Schema,
        interface: :advanced
      )
    end
  end

  scope "/*path" do
    pipe_through(:api)
    get("/", FallbackController, {:error, :not_found})
  end
end
