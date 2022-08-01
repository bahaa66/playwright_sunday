defmodule CogyntWorkstationIngest.Elasticsearch.Cluster do
  use Elasticsearch.Cluster, otp_app: :cogynt_workstation_ingest
  # TODO: Drop this and move completely to our cogynt-elasticsearch lib
end
