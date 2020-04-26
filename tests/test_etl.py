from acme.fraudcop.etl import *


class TestImput:
    def test_imput_on_nulls(self):
        k = "mykey"
        v = "imputed"
        inp = {k: None}
        rules = [Rule(key=k, imputed_value=v, apply_on_nulls=True)]
        assert imput(inp, rules) == {k: v}

    def test_imput_on_empty_strings(self):
        k = "mykey"
        v = "imputed"
        inp = {k: ""}
        rules = [Rule(key=k, imputed_value=v, apply_on_empty_strings=True)]
        assert imput(inp, rules) == {k: v}

    def test_imput_on_zeroes(self):
        k = "mykey"
        v = -1
        inp = {k: 0}
        rules = [Rule(key=k, imputed_value=v, apply_on_zeroes=True)]
        assert imput(inp, rules) == {k: v}

    def test_imput_on_negative_numbers(self):
        k = "mykey"
        v = 0
        inp = {k: -1}
        rules = [Rule(key=k, imputed_value=v, apply_on_negative_numbers=True)]
        assert imput(inp, rules) == {k: v}
