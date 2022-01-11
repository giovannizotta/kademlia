from kad.utils import *
import numpy as np


class RandomBatchGenerator:
    BATCH_SIZE = 10000
    _instance = None

    def __init__(self, seed=42):
        self._exponentials = {}
        self._choices = {}
        self._rng = np.random.default_rng(seed)

    def __new__(cls, **kwargs):
        """Manage singleton instance"""
        if cls._instance is None:
            cls._instance = super(RandomBatchGenerator, cls, **kwargs).__new__(cls)
        return cls._instance

    def get_exponential(self, mean: float) -> float:
        """Return an exponential with the given mean.

        The exponentials are precomputed in batch which is refreshed on demand.
        """
        mean = round(mean, 1)
        if not mean in self._exponentials:
            self._exponentials[mean] = iter([])
        try:
            exponential = next(self._exponentials[mean])
        except StopIteration:
            exp = self._rng.exponential(mean, RandomBatchGenerator.BATCH_SIZE)
            self._exponentials[mean] = iter(exp)
            exponential = next(self._exponentials[mean])
        return exponential

    def get_choice(self, container: Iterable[HashableT]) -> HashableT:
        """Return an random node from the container.

        The choices are precomputed in batch which is refreshed on demand.
        """
        if not isinstance(container, tuple):
            container = tuple(container)
        if not container in self._choices:
            self._choices[container] = iter([])
        try:
            choice = next(self._choices[container])
        except StopIteration:
            choices = self._rng.choice(container, RandomBatchGenerator.BATCH_SIZE)
            self._choices[container] = iter(choices)
            choice = next(self._choices[container])
        return choice
