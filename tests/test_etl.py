from acme.fraudcop.etl import *


class TestImput:
    # noinspection PyCallByClass
    def test_imput_on_nulls(self):
        k = "mykey"
        v = "imputed"
        inp = {k: None}
        rules = [Imput.Rule(key=k, imputed_value=v, apply_on_nulls=True)]
        assert Imput.transform(inp, rules) == {k: v}

    # noinspection PyCallByClass
    def test_imput_on_empty_strings(self):
        k = "mykey"
        v = "imputed"
        inp = {k: ""}
        rules = [Imput.Rule(key=k, imputed_value=v, apply_on_empty_strings=True)]
        assert Imput.transform(inp, rules) == {k: v}

    # noinspection PyCallByClass
    def test_imput_on_zeroes(self):
        k = "mykey"
        v = -1
        inp = {k: 0}
        rules = [Imput.Rule(key=k, imputed_value=v, apply_on_zeroes=True)]
        assert Imput.transform(inp, rules) == {k: v}

    # noinspection PyCallByClass
    def test_imput_on_negative_numbers(self):
        k = "mykey"
        v = 0
        inp = {k: -1}
        rules = [Imput.Rule(key=k, imputed_value=v, apply_on_negative_numbers=True)]
        assert Imput.transform(inp, rules) == {k: v}


class TestTransposeToKeyValueRows:
    def test_transpose(self):
        inp = {"id": 1, "col_1": "a"}
        assert TransposeToKeyValueRows.transform(
            inp, id_column="id", key_column="key", value_column="value"
        ) == [{"id": 1, "key": "col_1", "value": "a"}]

        inp = {"id": 1, "col_1": "a", "col_2": "aa"}
        assert TransposeToKeyValueRows.transform(
            inp, id_column="id", key_column="key", value_column="value"
        ) == [
            {"id": 1, "key": "col_1", "value": "a"},
            {"id": 1, "key": "col_2", "value": "aa"},
        ]
