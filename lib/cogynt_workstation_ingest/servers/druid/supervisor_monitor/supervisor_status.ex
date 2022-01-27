defmodule CogyntWorkstationIngest.Servers.Druid.SupervisorMonitor.SupervisorStatus do
  alias __MODULE__

  # Detailed supervisor states
  @unhealthy_supervisor_states [
    "UNHEALTHY_SUPERVISOR",
    "UNABLE_TO_CONNECT_TO_STREAM",
    "LOST_CONTACT_WITH_STREAM"
  ]

  @running_states [
    "CONNECTING_TO_STREAM",
    "DISCOVERING_INITIAL_TASKS",
    "CREATING_TASKS",
    "RUNNING"
  ]

  @enforce_keys [
    :data_source,
    :state,
    :detailed_state,
    :healthy,
    :suspended,
    :stream
  ]

  defstruct [
    :data_source,
    :state,
    :detailed_state,
    :healthy,
    :suspended,
    # The topic name
    :stream
  ]

  def new(params) do
    params =
      params
      |> Enum.map(fn {key, value} -> {String.to_atom(key), value} end)

    with {:ok, data_source} <- Keyword.fetch(params, :dataSource),
         {:ok, state} <- Keyword.fetch(params, :state),
         {:ok, detailed_state} <- Keyword.fetch(params, :detailedState),
         {:ok, healthy} <- Keyword.fetch(params, :healthy),
         {:ok, suspended} <- Keyword.fetch(params, :suspended),
         {:ok, stream} <- Keyword.fetch(params, :stream) do
      %SupervisorStatus{
        data_source: data_source,
        state: state,
        detailed_state: detailed_state,
        healthy: healthy,
        suspended: suspended,
        stream: stream
      }
    end
  end

  def is_running?(%SupervisorStatus{state: state, detailed_state: detailed}),
    do: state == "RUNNING" and detailed in @running_states

  def is_suspended?(%SupervisorStatus{state: state}),
    do: state == "SUSPENDED"

  def is_pending?(%SupervisorStatus{state: state}),
    do: state == "PENDING"

  def requires_reset?(%SupervisorStatus{state: state, detailed_state: detailed}),
    do: state == "UNHEALTHY_SUPERVISOR" and detailed in @unhealthy_supervisor_states
end
