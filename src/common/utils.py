from __future__ import annotations
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import *
import logging
import simpy
import numpy as np

# generic from hashable type
HashableT = TypeVar("HashableT", bound=Hashable)
# return type for simpy processes
T = TypeVar('T')
SimpyProcess = Generator[simpy.Event, simpy.Event, T]

class Singleton(type):
    """Singleton metaclass"""
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

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

    def get_choice(self, items: int | Tuple[HashableT]) -> HashableT:
        """Draw a random item from the range(items) if items is an int or from tuple"""
        if not items in self._choices:
            self._choices[items] = iter(())
        try:
            choice = next(self._choices[items])
        except StopIteration:
            choices = self._rng.choice(items, RandomBatchGenerator.BATCH_SIZE)
            self._choices[items] = iter(choices)
            choice = next(self._choices[items])
        return choice

@dataclass
class Loggable(ABC):
    """A class for objects that can log simulation events"""
    env: simpy.Environment = field(repr=False)
    name: str

    logger: logging.Logger = field(init=False, repr=False)
    rbg: RandomBatchGenerator = field(init=False, repr=False)

    @abstractmethod
    def __post_init__(self):
        self.logger = logging.getLogger("logger")
        self.rbg = RandomBatchGenerator()

    def log(self, msg: str, level: int = logging.DEBUG) -> None:
        """Log simulation events"""
        self.logger.log(level, f"{self.env.now:5.1f} {self.name:>12s}(id {self.id:50d}): {msg}")
