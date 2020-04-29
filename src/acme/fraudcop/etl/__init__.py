import math
from configparser import ConfigParser
from typing import NamedTuple, Dict, Any, Iterable, List

import apache_beam as beam

from acme.fraudcop.experiments import hash_to_float


class TableRef(NamedTuple):
    project: str
    dataset: str
    table: str

    # noinspection PyArgumentList
    # suppress warning: parameter `typename` unfilled
    @classmethod
    def from_qualified_name(cls, s: str):
        tokens = s.split(".")
        return cls(project=tokens[0], dataset=tokens[1], table=tokens[2])

    def qualified_name(self) -> str:
        return ".".join([self.project, self.dataset, self.table])


class ExecutionContext(NamedTuple):
    job_name: str
    conf: ConfigParser
    pipeline_args: List[str]


class AssignExperimentGroup(beam.PTransform):
    def __init__(self, input_key: str, output_key: str = "experiment_group_hash"):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key

    @staticmethod
    def transform(
        element: Dict[str, Any], input_key: str, output_key: str
    ) -> Dict[str, Any]:
        res = dict(element)
        res[output_key] = hash_to_float(res[input_key])
        return res

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.Map(
            AssignExperimentGroup.transform,
            input_key=self.input_key,
            output_key=self.output_key,
        )


class StandardDeviationCombineFn(beam.CombineFn):
    """
    The StandardDeviationCombineFn class is an accumulator that computes the sample standard deviation
    of a stream of real numbers. It provides an example of a mutable data type and a streaming
    algorithm.

    This implementation uses a one-pass algorithm that is less susceptible to floating-point roundoff error
    than the more straightforward implementation based on saving the sum of the squares of the numbers.

    Based on https://algs4.cs.princeton.edu/code/edu/princeton/cs/algs4/Accumulator.java.html
    """

    def to_runner_api_parameter(self, unused_context):
        raise NotImplementedError(str(self))

    def create_accumulator(self):
        return 0.0, 0.0, 0  # sum of deviations, mean, count

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        (sum_of_deviations, _mean, count) = mutable_accumulator
        count = count + 1
        # running mean
        _mean = (_mean * (count - 1) + element) / count
        sum_of_deviations = sum_of_deviations + (element - _mean) ** 2
        return sum_of_deviations, _mean, count

    def merge_accumulators(self, accumulators, **kwargs):
        sums_of_deviations, means, counts = zip(*accumulators)
        # drop the mean values because they are no longer needed
        return sum(sums_of_deviations), sum(counts)

    def extract_output(self, accumulator, **kwargs):
        (sum_of_deviations, count) = accumulator
        if count <= 1:
            std = float("NaN")
        else:
            variance = sum_of_deviations / (count - 1)
            # -ve value could happen due to rounding
            std = math.sqrt(variance) if variance > 0 else 0
        return {
            "std": std,
            "count": count,
        }


class Imput(beam.PTransform):
    class Rule(NamedTuple):
        key: str
        imputed_value: Any
        apply_on_nulls: bool = True
        apply_on_empty_strings: bool = False
        apply_on_zeroes: bool = False
        apply_on_negative_numbers: bool = False

    @staticmethod
    def transform(element: Dict[str, Any], rules: Iterable[Rule]) -> Dict[str, Any]:
        res = dict(element)
        for rule in rules:
            if rule.key not in res:
                continue
            v = res[rule.key]
            if rule.apply_on_nulls and v is None:
                res[rule.key] = rule.imputed_value
            if rule.apply_on_empty_strings and v == "":
                res[rule.key] = rule.imputed_value
            if rule.apply_on_zeroes and v == 0:
                res[rule.key] = rule.imputed_value
            if rule.apply_on_negative_numbers and v < 0:
                res[rule.key] = rule.imputed_value
        return res

    def __init__(self, rules: Iterable[Rule]):
        super().__init__()
        self.rules = rules

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.Map(Imput.transform, rules=self.rules)


class ImputStandardDeviation(beam.PTransform):
    def __init__(self, keys: Iterable[str]):
        super().__init__()
        self.keys = keys

    def expand(self, input_or_inputs):
        rules = []
        for k in self.keys:
            imputed_value = (
                input_or_inputs
                | beam.Map(lambda x: x[k])
                | beam.CombineGlobally(StandardDeviationCombineFn())
            )
            # noinspection PyCallByClass
            rules.append(Imput.Rule(key=k, imputed_value=imputed_value))

        return input_or_inputs | beam.Map(Imput.transform, rules=rules)


class TransposeToKeyValueRows(beam.PTransform):
    def __init__(
        self, id_column: str, key_column: str = "key", value_column: str = "value"
    ):
        super().__init__()
        self.id_column = id_column
        self.key_column = key_column
        self.value_column = value_column

    @staticmethod
    def transform(
        element: Dict[str, Any], id_column: str, key_column: str, value_column: str
    ) -> Iterable[Dict[str, Any]]:
        res = []
        _id = element[id_column]
        for k, v in element.items():
            if k == id_column:
                continue
            row = dict()
            row[id_column] = _id
            row[key_column] = k
            row[value_column] = v
            res.append(row)
        return res

    def expand(self, input_or_inputs):
        return input_or_inputs | beam.FlatMap(
            TransposeToKeyValueRows.transform,
            id_column=self.id_column,
            key_column=self.key_column,
            value_column=self.value_column,
        )
