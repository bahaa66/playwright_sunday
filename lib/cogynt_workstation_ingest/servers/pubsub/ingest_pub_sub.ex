defmodule CogyntWorkstationIngest.Servers.PubSub.IngestPubSub do
  @moduledoc """

  """
  use GenServer
  alias CogyntWorkstationIngest.Config
  alias CogyntWorkstationIngest.Deployments.DeploymentsContext
  alias CogyntWorkstationIngest.Utils.ConsumerStateManager
  alias CogyntWorkstationIngest.Supervisors.ConsumerGroupSupervisor

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
    {:ok, ref} = Redis.subscribe(pubsub, "ingest_channel", self())
    {:ok, %{ref: ref}}
  end

  @impl true
  def handle_info({:redix_pubsub, _pubsub, _ref, :subscribed, %{channel: channel}}, state) do
    CogyntLogger.info(
      "#{__MODULE__}",
      "Successfully subscribed to channel: #{inspect(channel, pretty: true)}"
    )

    {:noreply, state}
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pubsub, _ref, :message, %{channel: channel, payload: json_payload}},
        state
      ) do
    case Jason.decode(json_payload, keys: :atoms) do
      {:ok, %{start_consumer: event_definition} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        {:ok, created_at, _} = DateTime.from_iso8601(event_definition.created_at)
        {:ok, updated_at, _} = DateTime.from_iso8601(event_definition.updated_at)

        deleted_at =
          case is_nil(event_definition.deleted_at) do
            false ->
              {:ok, deleted_at, _} = DateTime.from_iso8601(event_definition.deleted_at)
              deleted_at

            true ->
              nil
          end

        event_definition =
          event_definition
          |> Map.put(:deleted_at, deleted_at)
          |> Map.put(:created_at, created_at)
          |> Map.put(:updated_at, updated_at)

        ConsumerStateManager.manage_request(%{start_consumer: event_definition})

      {:ok, %{start_drilldown_pipeline: deployment_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        deployment = DeploymentsContext.get_deployment(deployment_id)

        if not is_nil(deployment) do
          ConsumerGroupSupervisor.start_child(:drilldown, deployment)
        end

      {:ok, %{start_deployment_pipeline: _args} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        case ConsumerGroupSupervisor.start_child(:deployment) do
          {:error, nil} ->
            CogyntLogger.warn(
              "#{__MODULE__}",
              "Deployment Topic DNE. Adding to retry cache. Will reconnect once topic is created"
            )

            Redis.hash_set_async("crw", Config.deployment_topic(), "dp")

          _ ->
            Redis.hash_delete("crw", Config.deployment_topic())
            CogyntLogger.info("#{__MODULE__}", "Started Deployment Pipeline")
        end

      {:ok, %{stop_consumer: event_definition} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerStateManager.manage_request(%{stop_consumer: event_definition})

      {:ok, %{stop_deployment_pipeline: _args} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        ConsumerGroupSupervisor.stop_child(:deployment)

      {:ok, %{stop_drilldown_pipeline: deployment_id} = request} ->
        CogyntLogger.info(
          "#{__MODULE__}",
          "Channel: #{inspect(channel)}, Received message: #{inspect(request, pretty: true)}"
        )

        deployment = DeploymentsContext.get_deployment(deployment_id)

        if not is_nil(deployment) do
          ConsumerGroupSupervisor.stop_child(:drilldown, deployment)
        end

      {:ok, _} ->
        CogyntLogger.warn(
          "#{__MODULE__}",
          "Subscription carrying invalid payload. Does not match any command on IngestServer: #{
            inspect(json_payload, pretty: true)
          }"
        )

      {:error, error} ->
        CogyntLogger.error(
          "#{__MODULE__}",
          "Failed to decode subscription payload. Error: #{inspect(error, pretty: true)}"
        )
    end

    {:noreply, state}
  end
end
