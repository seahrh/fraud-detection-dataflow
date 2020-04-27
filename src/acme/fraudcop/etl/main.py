import argparse
from typing import NamedTuple, List
from configparser import ConfigParser, SectionProxy
from acme.fraudcop.etl import logging_pipeline


class ExecutionContext(NamedTuple):
    job_name: str
    conf: ConfigParser
    pipeline_args: List[str]


def _pipeline_args(job_name: str, conf: SectionProxy) -> List[str]:
    """Arguments required by Dataflow runner"""
    return [
        f'--project={conf["project"]}',
        f'--region={conf["region"]}',
        f'--temp_location={conf["temp_location"]}',
        f'--runner={conf["runner"]}',
        f'--setup_file={conf["setup_file"]}',
        f"--job_name={job_name}",
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
    # conf.set(args.job_name, "job_name", args.job_name)
    return ExecutionContext(
        job_name=args.job_name,
        conf=conf,
        pipeline_args=_pipeline_args(job_name=args.job_name, conf=conf["dataflow"]),
    )


def _main(argv=None) -> None:
    context = _parse(argv)
    if context.job_name == context.conf["evaluation-metrics-logging"]["job_name"]:
        logging_pipeline.run(context)
        return
    raise ValueError(f"Unrecognized job name: {context.job_name}")


if __name__ == "__main__":
    _main()