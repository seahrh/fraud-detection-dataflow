import math
from acme.fraudcop.experiments import *


class TestHashToFloat:
    EPSILON = 0.001
    N_TRIALS = 20

    def test_result_must_be_a_decimal_between_zero_and_one_inclusive(self):
        inp = "2019-12-31T23:59:59"
        for _ in range(self.N_TRIALS):
            assert 0 <= hash_to_float(inp) <= 1

    def test_difference_between_repeated_hashes_must_not_exceed_epsilon(self):
        inp = "2019-12-31T23:59:59"
        for _ in range(self.N_TRIALS):
            assert math.fabs(hash_to_float(inp) - hash_to_float(inp)) <= self.EPSILON
