import logging
from typing import Dict, Any
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions


from acme.fraudcop.etl.main import ExecutionContext
from acme.fraudcop.metrics import metric_pb2, to_named_tuple

_log = logging.getLogger(__name__)


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size: int):
        super().__init__()
        self.window_size = window_size

    @staticmethod
    def transform(element: bytes) -> Dict[str, Any]:
        _log.info(f"element={repr(element)}")
        m = metric_pb2.Metric()
        m.ParseFromString(element)
        res = to_named_tuple(m)._asdict()
        _log.info(f"res={repr(res)}")
        return res

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its publish timestamp.
            | "window_into" >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "parse_message" >> beam.Map(GroupWindowsIntoBatches.transform)
        )


def run(context: ExecutionContext) -> None:
    """The main function which creates the pipeline and runs it."""
    sink_table = context.conf[context.job_name]["sink_table"]
    sink_dataset = context.conf[context.job_name]["sink_dataset"]
    sink_project = context.conf[context.job_name]["sink_project"]
    source_topic = context.conf[context.job_name]["source_topic"]
    window_size_seconds = int(context.conf[context.job_name]["window_size_seconds"])
    options = PipelineOptions(context.pipeline_args, streaming=True)
    _log.info(f"PipelineOptions={options.display_data()}")

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "read_pubsub" >> beam.io.ReadFromPubSub(topic=source_topic)
            | "process_messages" >> GroupWindowsIntoBatches(window_size_seconds)
        )
