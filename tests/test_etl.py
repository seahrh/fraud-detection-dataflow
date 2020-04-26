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
