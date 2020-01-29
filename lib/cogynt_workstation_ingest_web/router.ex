defmodule CogyntWorkstationIngestWeb.Router do
  use CogyntWorkstationIngestWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/api", CogyntWorkstationIngestWeb do
    pipe_through :api
  end
end
