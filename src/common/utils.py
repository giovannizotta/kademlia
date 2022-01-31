from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import *

import numpy as np
import simpy
import simpy.events
from dataclasses import dataclass, field

# generic from hashable type
HashableT = TypeVar("HashableT", bound=Hashable)
# return type for simpy processes
T = TypeVar('T')
SimpyProcess = Generator[Union[simpy.Event,
                               simpy.Process], simpy.events.ConditionValue, T]

class DHTTimeoutError(Exception):
    pass

class Singleton(type):
    __instances: Dict = {}

    def __call__(cls, *args, **kwargs):
        if cls not in Singleton.__instances:
            instance = super(Singleton, cls).__call__(*args, **kwargs)
            Singleton.__instances[cls] = instance
        return Singleton.__instances[cls]


@dataclass
class RandomBatchGenerator(metaclass=Singleton):
    """Generate random numbers in batch in an optimized way.

    The random samples are precomputed in batch and refreshed on demand.
    """
    BATCH_SIZE = 10000
    _instance = None

    def __init__(self, seed: int = 42, precision: int = 1):
        """Initialize Random Batch Generator

        Args:
            seed (int, optional): Random seed to use. Defaults to 42.
            precision (int, optional): Decimal precision of distributions' parameters. Defaults to 1.
        """
        self._exponentials: Dict[int, Iterator[float]] = {}
        self._choices: Dict[int, Iterator[int]] = {}
        self._rng: np.random.Generator = np.random.default_rng(seed)
        self.precision: int = 10 ** precision

    def get_exponential(self, mean: float) -> float:
        """Draw a number from an exponential with the given mean"""
        # use ints as keys
        mean = round(mean * self.precision)
        if not mean in self._exponentials:
            self._exponentials[mean] = iter(np.ndarray(0))
        try:
            sample = next(self._exponentials[mean])
        except StopIteration:
            exp = self._rng.exponential(
                mean / self.precision, RandomBatchGenerator.BATCH_SIZE)
            self._exponentials[mean] = iter(exp)
            sample = next(self._exponentials[mean])
        return sample
        # return exponential

    def get_from_range(self, n: int) -> int:
        """Draw a random item from the range(n)"""
        if not n in self._choices:
            self._choices[n] = iter(np.ndarray(0))
        try:
            choice = next(self._choices[n])
        except StopIteration:
            choices = self._rng.choice(n, RandomBatchGenerator.BATCH_SIZE)
            self._choices[n] = iter(choices)
            choice = next(self._choices[n])
        return choice


@dataclass
class Loggable(ABC):
    """A class for objects that can log simulation events"""
    env: simpy.Environment = field(repr=False)
    name: str

    id: int = field(init=False)

    logger: logging.Logger = field(init=False, repr=False)
    rbg: RandomBatchGenerator = field(init=False, repr=False)

    @abstractmethod
    def __post_init__(self) -> None:
        self.logger = logging.getLogger("logger")
        self.rbg = RandomBatchGenerator()

    def log(self, msg: str, level: int = logging.DEBUG) -> None:
        """Log simulation events"""
        self.logger.log(
            level, f"{self.env.now:5.1f} {self.name:>12s}(id {self.id:50d}): {msg}")
