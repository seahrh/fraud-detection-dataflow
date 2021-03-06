import argparse
from typing import List
from configparser import ConfigParser
from acme.fraudcop.etl import ExecutionContext, metrics_pipeline, inference_pipeline


def _pipeline_args(
    job_name: str,
    project: str,
    region: str,
    temp_location: str,
    runner: str,
    setup_file: str,
    machine_type: str,
) -> List[str]:
    """Arguments required by Dataflow runner"""
    return [
        f"--project={project}",
        f"--region={region}",
        f"--temp_location={temp_location}",
        f"--runner={runner}",
        f"--setup_file={setup_file}",
        f"--job_name={job_name}",
        f"--machine_type={machine_type}",
    ]


def _parse(argv):
    """Overwrite config with values from commandline arguments."""
    conf = ConfigParser()
    conf.read("app.ini")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--job_name",
        dest="job_name",
        required=True,
        help="name of the dataflow job to run",
    )
    args, _ = parser.parse_known_args(argv)
    return ExecutionContext(
        job_name=args.job_name,
        conf=conf,
        pipeline_args=_pipeline_args(
            job_name=args.job_name,
            project=conf["dataflow"]["project"],
            region=conf["dataflow"]["region"],
            temp_location=conf["dataflow"]["temp_location"],
            runner=conf["dataflow"]["runner"],
            setup_file=conf["dataflow"]["setup_file"],
            machine_type=conf["dataflow"]["machine_type"],
        ),
    )


def _main(argv=None) -> None:
    context = _parse(argv)
    if context.job_name == context.conf["evaluation-metrics-logging"]["job_name"]:
        metrics_pipeline.run(context)
        return
    if context.job_name == context.conf["fraud-detection"]["job_name"]:
        inference_pipeline.run(context)
        return
    raise ValueError(f"Unrecognized job name: {context.job_name}")


if __name__ == "__main__":
    _main()
