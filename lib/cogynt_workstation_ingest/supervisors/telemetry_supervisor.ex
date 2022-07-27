defmodule CogyntWorkstationIngest.Supervisors.TelemetrySupervisor do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg) do
    Supervisor.start_link(__MODULE__, arg, name: __MODULE__)
  end

  def init(_arg) do
    children = [
      {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    [
      # Phoenix Metrics
      summary("phoenix.endpoint.stop.duration",
        unit: {:native, :millisecond}
      ),
      summary("phoenix.router_dispatch.stop.duration",
        tags: [:route],
        unit: {:native, :millisecond}
      ),

      # Erlang VM Metrics - Formats `gauge` metric type
      last_value("vm.memory.total", unit: {:byte, :megabyte}),
      last_value("vm.total_run_queue_lengths.total"),
      last_value("vm.total_run_queue_lengths.cpu"),
      last_value("vm.system_counts.process_count"),

      # Database Time Metrics - Formats `timing` metric type
      summary(
        "cogynt_workstation_ingest.repo.query.idle_time",
        unit: {:native, :millisecond},
        tags: [:source, :repo]
      ),
      summary("cogynt_workstation_ingest.repo.query.queue_time",
        unit: {:native, :millisecond},
        tags: [:source, :repo]
      ),
      summary("cogynt_workstation_ingest.repo.query.query_time",
        unit: {:native, :millisecond},
        tags: [:source, :repo]
      ),
      summary("cogynt_workstation_ingest.repo.query.decode_time",
        unit: {:native, :millisecond},
        tags: [:source, :repo]
      ),
      summary("cogynt_workstation_ingest.repo.query.total_time",
        unit: {:native, :millisecond},
        tags: [:source, :repo]
      ),

      # Database Count Metrics - Formats `count` metric type
      counter(
        "cogynt_workstation_ingest.repo.query.count",
        tags: [:source, :repo]
      ),

      # Phoenix Time Metrics - Formats `timing` metric type
      summary(
        "phoenix.router_dispatch.stop.duration",
        unit: {:native, :millisecond}
      ),

      # Phoenix Count Metrics - Formats `count` metric type
      counter("phoenix.router_dispatch.stop.count"),
      counter("phoenix.error_rendered.count"),

      # Broadway Metrics
      summary(
        "broadway.processor.message.stop.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.batch_processor.stop.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.process_event.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.process_event_history.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.process_elasticsearch_documents.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.process_notifications.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.event_processor_all_crud_processing_stages.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.event_processor_all_processing_stages.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.execute_batch_transaction_success.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.execute_batch_transaction_failed.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "broadway.process_entities.duration",
        unit: {:native, :millisecond}
      ),

      # Redis Metrics
      summary(
        "redix.hash_set.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.hash_get.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.list_trim.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.list_range.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.list_append.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.list_length.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.stream_add.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.stream_length.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.stream_trim.duration",
        unit: {:native, :millisecond}
      ),
      summary(
        "redix.stream_read.duration",
        unit: {:native, :millisecond}
      )
    ]
  end

  defp periodic_measurements do
    []
  end
end
