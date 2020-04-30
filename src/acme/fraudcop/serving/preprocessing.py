from datetime import datetime, date, timezone
from decimal import Decimal
from configparser import ConfigParser
from typing import Dict, Union, Optional, NamedTuple

_conf = ConfigParser()
_conf.read("serving.ini")
# TODO read env value from environment variable
DATE_FORMAT = _conf[__name__]["date_format"]
NULL_ENCODING_KEY = _conf[__name__]["null_encoding_key"]
DAILY_SPEND = float(_conf[__name__]["average_daily_user_spend"])


def _as_dict(encoding: str) -> Dict[str, float]:
    entries = encoding.split()
    res = {}
    for e in entries:
        k, v = e.split(":")
        res[k] = float(v)
    return res


TYPE_ENCODING = _as_dict(_conf[__name__]["type_encoding"])
AMOUNT_STATS = _as_dict(_conf[__name__]["amount_stats"])
CARD_AGE_STATS = _as_dict(_conf[__name__]["card_age_stats"])


class Features(NamedTuple):
    date_day_of_week: int
    date_week_number: int
    type: float
    amount: float
    amount_to_daily_spend: float
    a4: float
    card_age: float


class Example(NamedTuple):
    date: int
    amount: Decimal  # demo standardization
    type: Optional[str] = None
    a4: Optional[int] = None  # demo imput mean
    card_issued: Optional[str] = None


def _date(s: str) -> date:
    return datetime.strptime(s, DATE_FORMAT).astimezone(timezone.utc).date()


def _standard_scale(
    v: Optional[Union[float, Decimal]], mean: float, std: float
) -> float:
    v = v if v is not None else mean
    return (float(v) - mean) / std


def _card_age(card_issued: str, txn_date: date) -> int:
    card_date = _date(card_issued[:6])
    return (txn_date - card_date).days


def preprocess(example: Example) -> Features:
    d = _date(str(example.date))
    card_age = None
    if example.card_issued is not None:
        card_age = _card_age(card_issued=example.card_issued, txn_date=d)
    _type = example.type if example.type is not None else NULL_ENCODING_KEY
    return Features(
        date_day_of_week=d.isoweekday(),
        date_week_number=d.isocalendar()[1],
        type=TYPE_ENCODING[_type],
        amount=_standard_scale(
            float(example.amount), AMOUNT_STATS["mean"], AMOUNT_STATS["std"]
        ),
        amount_to_daily_spend=float(example.amount) / DAILY_SPEND,
        a4=_standard_scale(example.a4, AMOUNT_STATS["mean"], AMOUNT_STATS["std"]),
        card_age=_standard_scale(
            card_age, CARD_AGE_STATS["mean"], CARD_AGE_STATS["std"]
        ),
    )
