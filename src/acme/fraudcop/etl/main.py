import argparse
from typing import NamedTuple, List
from configparser import ConfigParser, SectionProxy
from acme.fraudcop.etl import logging_pipeline


class ExecutionContext(NamedTuple):
    job_name: str
    conf: ConfigParser
    pipeline_args: List[str]


def _pipeline_args(
    job_name: str,
    project: str,
    region: str,
    temp_location: str,
    runner: str,
    setup_file: str,
    streaming: bool = False,
) -> List[str]:
    """Arguments required by Dataflow runner"""
    res = [
        f"--project={project}",
        f"--region={region}",
        f"--temp_location={temp_location}",
        f"--runner={runner}",
        f"--setup_file={setup_file}",
        f"--job_name={job_name}",
    ]
    if streaming:
        res.append("--streaming")
    return res


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
        pipeline_args=_pipeline_args(
            job_name=args.job_name,
            project=conf["dataflow"]["project"],
            region=conf["dataflow"]["region"],
            temp_location=conf["dataflow"]["temp_location"],
            runner=conf["dataflow"]["runner"],
            setup_file=conf["dataflow"]["setup_file"],
            streaming=conf["dataflow"].getboolean("streaming"),
        ),
    )


def _main(argv=None) -> None:
    context = _parse(argv)
    if context.job_name == context.conf["evaluation-metrics-logging"]["job_name"]:
        logging_pipeline.run(context)
        return
    raise ValueError(f"Unrecognized job name: {context.job_name}")


if __name__ == "__main__":
    _main()
