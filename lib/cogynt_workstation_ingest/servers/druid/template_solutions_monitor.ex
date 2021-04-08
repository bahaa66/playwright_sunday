defmodule CogyntWorkstationIngest.Servers.Druid.TemplateSolutions do
  use GenServer
  alias CogyntWorkstationIngest.Config

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(_arg) do
    brokers =
      Config.kafka_brokers()
      |> Enum.map(fn {host, port} -> "#{host}:#{port}" end)
      |> Enum.join(",")

    with {:ok, true} <- Druid.status_health(),
         {:error, %{code: 400, error: "Cannot find any supervisor with id" <> _}} <-
           Druid.get_supervisor(Config.template_solutions_topic()),
         supervisor <-
           Druid.Utils.base_kafka_supervisor(Config.template_solutions_topic(), brokers) do
      IO.inspect(supervisor, label: "GOOD TO GO")
      {:ok, %{}}
    else
      {:ok, template_solutions_supervisor} ->
        IO.inspect(template_solutions_supervisor)
        {:ok, %{druid_supervisor: template_solutions_supervisor}}

      {:error, error} ->
        IO.inspect(error)

        CogyntLogger.error(
          "#{__MODULE__}",
          "Unable to verify the health of the #{inspect(error)}"
        )

        {:stop, :unhealthy_druid_server}
    end
  end
end
