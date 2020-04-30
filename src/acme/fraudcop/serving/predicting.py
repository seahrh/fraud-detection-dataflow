from acme.fraudcop.serving.preprocessing import Features


# noinspection PyMethodMayBeStatic,PyNoneFunctionAssignment
class Model:
    def __init__(self, path: str):
        self.path = path
        self.model = self._load()

    def _load(self):
        """Load model from file path."""
        return None

    def predict(self, features: Features) -> float:
        import random

        random.seed(len(features))
        return random.random()
