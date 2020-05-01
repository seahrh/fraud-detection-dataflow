import random
from acme.fraudcop.serving.preprocessing import Features


# noinspection PyMethodMayBeStatic,PyNoneFunctionAssignment
class Model:
    def __init__(self, path: str):
        self.path = path
        self.model = self._load()

    def _load(self):
        """Load model from file path."""
        random.seed(1)
        return None

    def predict(self, features: Features) -> float:
        return random.random()
