from common.utils import *
import numpy as np


class RandomBatchGenerator(metaclass=Singleton):
    """Generate random numbers in batch in an optimized way.

    The random samples are precomputed in batch and refreshed on demand.
    """
    BATCH_SIZE = 10000
    _instance = None

    def __init__(self, seed: int=42, precision: int=1):
        """Initialize Random Batch Generator

        Args:
            seed (int, optional): Random seed to use. Defaults to 42.
            precision (int, optional): Decimal precision of distributions' parameters. Defaults to 1.
        """
        self._exponentials = {}
        self._choices = {}
        self._rng = np.random.default_rng(seed)
        self.precision = 10**precision

    def get_exponential(self, mean: float) -> float:
        """Draw a number from an exponential with the given mean"""
        # use ints as keys
        mean = round(mean * self.precision)
        if not mean in self._exponentials:
            self._exponentials[mean] = iter(())
        try:
            exponential = next(self._exponentials[mean])
        except StopIteration:
            exp = self._rng.exponential(mean/self.precision, RandomBatchGenerator.BATCH_SIZE)
            self._exponentials[mean] = iter(exp)
            exponential = next(self._exponentials[mean])
        return exponential

    def get_choice(self, container: Iterable[HashableT]) -> HashableT:
        """Draw a random item from the container"""
        if not isinstance(container, tuple):
            container = tuple(container)
        if not container in self._choices:
            self._choices[container] = iter(())
        try:
            choice = next(self._choices[container])
        except StopIteration:
            choices = self._rng.choice(container, RandomBatchGenerator.BATCH_SIZE)
            self._choices[container] = iter(choices)
            choice = next(self._choices[container])
        return choice
