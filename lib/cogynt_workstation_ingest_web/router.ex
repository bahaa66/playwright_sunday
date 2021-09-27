defmodule CogyntWorkstationIngestWeb.Router do
  use CogyntWorkstationIngestWeb, :router
  import Phoenix.LiveDashboard.Router
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Supervisors.TelemetrySupervisor
  alias CogyntWorkstationIngestWeb.FallbackController

  pipeline :api do
    plug(:accepts, ["json"])
    plug(:fetch_session)
  end

  pipeline :browser do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  scope "/" do
    pipe_through(:browser)

    if Config.enable_dev_tools?() do
      live_dashboard("/dashboard", metrics: TelemetrySupervisor)
    end
  end

  ## Health Check route
  forward("/healthz", HealthCheckup)
  ## Liveness Check route
  forward("/livenessCheck", LivenessCheck)

  scope "/api", CogyntWorkstationIngestWeb do
    pipe_through(:api)
    get("/drilldown/all/:id", DrilldownController, :index)
    get("/drilldown/:id", DrilldownController, :show)
  end

  scope "/api" do
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
    get "/", FallbackController, {:error, :not_found}
  end
end
