defmodule CogyntWorkstationIngestWeb.Router do
  use CogyntWorkstationIngestWeb, :router

  pipeline :api do
    plug(:accepts, ["json"])
  end
  
  ## Health Check route
  forward "/healthz", HealthCheckup
  
  scope "/api", CogyntWorkstationIngestWeb do
    pipe_through :api
    get "/drilldown/all/:id", DrilldownController, :index
    get "/drilldown/:id", DrilldownController, :show
  end
end
