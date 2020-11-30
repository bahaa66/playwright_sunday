defmodule CogyntWorkstationIngest.Utils.JobQueue.CustomNodeIdentifier do
  @behaviour Exq.NodeIdentifier.Behaviour

  def node_id do
    "cogynt-ws-ingest-otp"
  end
end
