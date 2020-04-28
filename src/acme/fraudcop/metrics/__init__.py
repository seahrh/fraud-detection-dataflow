from datetime import datetime
from typing import NamedTuple


class Metric(NamedTuple):
    metric: str
    metric_value: float
    test_id: str
    test_time: datetime
    mode: str
    version: str
    model_name: str
    model_uri: str


def to_named_tuple(pb2_obj) -> Metric:
    """Converts the Metric proto object to a NamedTuple."""
    return Metric(
        metric=pb2_obj.metric,
        metric_value=pb2_obj.metric_value,
        test_id=pb2_obj.test_id,
        test_time=datetime.utcfromtimestamp(pb2_obj.test_time),
        mode=pb2_obj.mode,
        version=pb2_obj.version,
        model_name=pb2_obj.model_name,
        model_uri=pb2_obj.model_uri,
    )
