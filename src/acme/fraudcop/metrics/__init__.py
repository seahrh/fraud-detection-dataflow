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

    # noinspection PyArgumentList
    # suppress warning: parameter `typename` unfilled
    @classmethod
    def from_protobuf(cls, obj):
        """Converts the Metric proto object to a NamedTuple."""
        return cls(
            metric=obj.metric,
            metric_value=obj.metric_value,
            test_id=obj.test_id,
            test_time=datetime.utcfromtimestamp(obj.test_time),
            mode=obj.mode,
            version=obj.version,
            model_name=obj.model_name,
            model_uri=obj.model_uri,
        )
