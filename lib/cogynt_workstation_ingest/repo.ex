defmodule CogyntWorkstationIngest.Repo do
  use Ecto.Repo,
    otp_app: :cogynt_workstation_ingest,
    adapter: Ecto.Adapters.Postgres
end
