defmodule CogyntWorkstationIngest.Repo do
  use Ecto.Repo,
    otp_app: :cogynt_workstation_ingest,
    adapter: Ecto.Adapters.Postgres

  use Scrivener, page_size: 10
end
