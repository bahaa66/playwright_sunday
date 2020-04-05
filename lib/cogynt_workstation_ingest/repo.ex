defmodule CogyntWorkstationIngest.Repo do
  use Ecto.Repo,
    otp_app: :cogynt_workstation_ingest,
    adapter: Ecto.Adapters.Postgres

  def stream_preload(stream, size, preloads) do
    stream
    |> Stream.chunk_every(size)
    |> Stream.flat_map(fn chunk ->
      CogyntWorkstationIngest.Repo.preload(chunk, preloads)
    end)
  end
end
