from datetime import datetime
from typing import Dict, Any, Iterable

import apache_beam as beam
from apache_beam import Pipeline, PTransform
from apache_beam.options.pipeline_options import PipelineOptions
from acme.fraudcop import cloud_logger

_log = cloud_logger(__name__)


def run(
    sink_project: str,
    sink_dataset: str,
    sink_table: str,
    columns: Iterable[str],
    pipeline_args: Iterable[str],
) -> None:
    """The main function which creates the pipeline and runs it."""
    if len(list(columns)) == 0:
        raise ValueError("Columns must not be empty.")
    job_name = f"evaluation-metrics-logging"
    args = list(pipeline_args)
    args.append(f"--job_name={job_name}")
    options = PipelineOptions(args)
    _log.info(f"PipelineOptions={options.display_data()}")
    p = beam.Pipeline(options=options)
    sink = beam.io.Write(
        beam.io.WriteToBigQuery(
            sink_table,
            dataset=sink_dataset,
            project=sink_project,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )

    p.run().wait_until_finish()
