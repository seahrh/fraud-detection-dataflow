from datetime import datetime
from typing import Dict, Any, Iterable

import apache_beam as beam
from apache_beam import Pipeline, PTransform
from apache_beam.options.pipeline_options import PipelineOptions
from acme.fraudcop import cloud_logger
from acme.fraudcop.etl.main import ExecutionContext

# _log = cloud_logger(__name__)


def run(context: ExecutionContext) -> None:
    """The main function which creates the pipeline and runs it."""
    print("hello logging_pipeline")
