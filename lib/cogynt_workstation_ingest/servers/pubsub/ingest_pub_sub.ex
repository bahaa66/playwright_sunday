defmodule CogyntWorkstationIngest.Servers.PubSub.IngestPubSub do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Config

  # -------------------- #
  # --- client calls --- #
  # -------------------- #
  def start_link(pubsub) do
    GenServer.start_link(__MODULE__, pubsub, name: __MODULE__)
  end

  # ------------------------ #
  # --- server callbacks --- #
  # ------------------------ #
  @impl true
  def init(pubsub) do
    {:ok, ref} = Redix.PubSub.subscribe(pubsub, "ingest_channel", self())
    {:ok, %{ref: ref}}
  end

  @impl true
  def handle_info({:redix_pubsub, _pubsub, _ref, :subscribed, %{channel: channel}}, state) do
    CogyntLogger.info("#{__MODULE__}", "Successfully subscribed to channel: #{inspect(channel)}")
    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub, _ref, :message, %{channel: channel, payload: payload}},
        state
      ) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Channel: #{inspect(channel)}, Received message: #{inspect(payload)}"
    )

    {:noreply, state}
  end
end
