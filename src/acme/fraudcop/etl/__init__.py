import math
from typing import NamedTuple, Dict, Any, Iterable
import apache_beam as beam


class Rule(NamedTuple):
    key: str
    imputed_value: Any
    apply_on_nulls: bool = True
    apply_on_empty_strings: bool = False
    apply_on_zeroes: bool = False
    apply_on_negative_numbers: bool = False


def imput(element: Dict[str, Any], rules: Iterable[Rule]) -> Dict[str, Any]:
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


class StandardDeviationCombiner(beam.CombineFn):
    def to_runner_api_parameter(self, unused_context):
        raise NotImplementedError(str(self))

    def create_accumulator(self):
        return 0.0, 0.0, 0  # sum of deviations, mean, count

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        (sum_of_deviations, _mean, count) = mutable_accumulator
        count = count + 1
        # running mean
        _mean = _mean * (count - 1) / count + (element / count)
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
