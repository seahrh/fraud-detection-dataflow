import logging
import google.cloud.logging  # Don't conflict with standard logging
from google.cloud.logging.handlers import CloudLoggingHandler
from .experiments import *

__all__ = ["cloud_logger"]
__all__ += experiments.__all__  # type: ignore  # Name is not defined


def cloud_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client)
    res = logging.getLogger(name)
    res.setLevel(level)  # defaults to WARN
    res.addHandler(handler)
    return res
