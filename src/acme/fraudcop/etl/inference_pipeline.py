import logging
from typing import Dict, Any, Iterable
import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions

from acme.fraudcop.etl import ExecutionContext, TableRef, AssignExperimentGroup
from acme.fraudcop.transactions import transaction_pb2, Transaction
from acme.fraudcop.serving.preprocessing import Example, preprocess, Features

_log = logging.getLogger(__name__)


class ParseMessages(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries.
    """

    def __init__(self, window_size: int):
        super().__init__()
        self.window_size = window_size

    @staticmethod
    def transform(element: bytes) -> Dict[str, Any]:
        _log.debug(f"element={repr(element)}")
        pb = transaction_pb2.Transaction()
        pb.ParseFromString(element)
        # noinspection PyProtectedMember
        # suppress warning: _asdict is the only fix for NamedTuple in Python3.6+
        res: Dict[str, Any] = Transaction.from_protobuf(pb)._asdict()
        _log.info(f"res={repr(res)}")
        return res

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its publish timestamp.
            | "window_into" >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "parse_message" >> beam.Map(ParseMessages.transform)
        )


class ScrubBlacklist(beam.PTransform):
    """A composite transform that marks transaction as fraud if blacklist rule applies.
    """

    def __init__(self, districts, cards):
        super().__init__()
        self.districts = districts
        self.cards = cards

    @staticmethod
    def transform(
        element: Dict[str, Any], district_ids: Iterable[int], card_ids: Iterable[int]
    ) -> Dict[str, Any]:
        districts = set(district_ids)
        cards = set(card_ids)
        _log.debug(
            f"element={repr(element)}\ndistricts={repr(districts)}\ncards={repr(cards)}"
        )
        res = dict(element)
        res["blacklist_is_fraud"] = False
        res["blacklist_reason"] = ""
        district = res["district_id"]
        if district in districts:
            res["blacklist_is_fraud"] = True
            res["blacklist_reason"] = f"district id {district}"
        card = res["card_id"]
        if card in cards:
            res["blacklist_is_fraud"] = True
            res["blacklist_reason"] = f"card id {card}"
        _log.info(f"res={repr(res)}")
        return res

    def expand(self, pcoll):
        return pcoll | "check_blacklist" >> beam.Map(
            ScrubBlacklist.transform,
            beam.pvalue.AsIter(self.districts),
            beam.pvalue.AsIter(self.cards),
        )


class MakeFeatures(beam.PTransform):
    """A composite transform that performs feature engineering.
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def transform(element: Dict[str, Any]) -> Dict[str, Any]:
        _log.debug(f"element={repr(element)}")
        features: Features = preprocess(
            Example(
                date=element["date"],
                type=element["type"],
                amount=element["amount"],
                a4=element["a4"],
                card_issued=element["card_issued"],
            )
        )
        res = dict(element)
        # noinspection PyProtectedMember
        # suppress warning: _asdict is the only fix for NamedTuple in Python3.6+
        for k, v in features._asdict():
            res[f"f_{k}"] = v
        _log.info(f"res={repr(res)}")
        return res

    def expand(self, pcoll):
        return pcoll | "preprocess" >> beam.Map(MakeFeatures.transform)


def run(context: ExecutionContext) -> None:
    """The main function which creates the pipeline and runs it."""
    sink_table = TableRef.from_qualified_name(
        context.conf[context.job_name]["sink_table"]
    )
    blacklist_districts_file = context.conf[context.job_name][
        "blacklist_districts_file"
    ]
    blacklist_cards_file = context.conf[context.job_name]["blacklist_cards_file"]
    experiment_hash_input_key = context.conf[context.job_name][
        "experiment_hash_input_key"
    ]
    source_topic = context.conf[context.job_name]["source_topic"]
    window_size_seconds = int(context.conf[context.job_name]["window_size_seconds"])
    options = PipelineOptions(context.pipeline_args, streaming=True)
    _log.info(f"PipelineOptions={options.display_data()}")

    with beam.Pipeline(options=options) as pipeline:
        districts = (
            pipeline
            | "district_blacklist" >> beam.io.ReadFromText(blacklist_districts_file)
            | "district_blacklist_type_cast" >> beam.Map(int)
        )
        cards = (
            pipeline
            | "card_blacklist" >> beam.io.ReadFromText(blacklist_cards_file)
            | "card_blacklist_type_cast" >> beam.Map(int)
        )
        (
            pipeline
            | "read_pubsub" >> beam.io.ReadFromPubSub(topic=source_topic)
            | "parse_messages" >> ParseMessages(window_size_seconds)
            | "scrub_blacklist" >> ScrubBlacklist(districts=districts, cards=cards)
            | "assign_experiment_group"
            >> AssignExperimentGroup(input_key=experiment_hash_input_key)
            | "make_features" >> MakeFeatures()
        )
