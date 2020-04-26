from typing import NamedTuple, Dict, Any, Iterable


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
