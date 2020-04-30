from datetime import datetime, date, timezone
from decimal import Decimal
from configparser import ConfigParser
from typing import Dict, Union, Optional, NamedTuple


def _as_dict(encoding: str) -> Dict[str, float]:
    entries = encoding.split()
    res = {}
    for e in entries:
        k, v = e.split(":")
        res[k] = float(v)
    return res


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


def _date(s: str, date_format: str) -> date:
    return datetime.strptime(s, date_format).astimezone(timezone.utc).date()


def _standard_scale(
    v: Optional[Union[float, Decimal]], mean: float, std: float
) -> float:
    v = v if v is not None else mean
    return (float(v) - mean) / std


def _card_age(card_issued: str, date_format: str, txn_date: date) -> int:
    card_date = _date(card_issued[:6], date_format)
    return (txn_date - card_date).days


def preprocess(example: Example, conf: ConfigParser) -> Features:
    date_format = conf[__name__]["date_format"]
    null_encoding_key = conf[__name__]["null_encoding_key"]
    daily_spend = float(conf[__name__]["average_daily_user_spend"])
    type_encoding = _as_dict(conf[__name__]["type_encoding"])
    amount_stats = _as_dict(conf[__name__]["amount_stats"])
    card_age_stats = _as_dict(conf[__name__]["card_age_stats"])
    d = _date(str(example.date), date_format)
    card_age = None
    if example.card_issued is not None:
        card_age = _card_age(
            card_issued=example.card_issued, date_format=date_format, txn_date=d
        )
    _type = example.type if example.type is not None else null_encoding_key
    return Features(
        date_day_of_week=d.isoweekday(),
        date_week_number=d.isocalendar()[1],
        type=type_encoding[_type],
        amount=_standard_scale(
            float(example.amount), amount_stats["mean"], amount_stats["std"]
        ),
        amount_to_daily_spend=float(example.amount) / daily_spend,
        a4=_standard_scale(example.a4, amount_stats["mean"], amount_stats["std"]),
        card_age=_standard_scale(
            card_age, card_age_stats["mean"], card_age_stats["std"]
        ),
    )
