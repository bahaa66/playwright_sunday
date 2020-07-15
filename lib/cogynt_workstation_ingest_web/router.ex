defmodule CogyntWorkstationIngestWeb.Router do
  use CogyntWorkstationIngestWeb, :router
  import Phoenix.LiveDashboard.Router

  alias CogyntWorkstationIngest.Supervisors.TelemetrySupervisor

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

  if Mix.env() == :dev do
    scope "/" do
      pipe_through(:browser)
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
end
