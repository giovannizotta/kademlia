from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from itertools import count
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Generator,
    Hashable,
    Iterator,
    List,
    NewType,
    Tuple,
    TypeVar,
)

import numpy as np
import simpy
import simpy.events
from simpy.core import Environment
from simpy.events import Event

# generic from hashable type
if TYPE_CHECKING:
    from common.node import DHTNode

HashableT = TypeVar("HashableT", bound=Hashable)
# return type for simpy processes
T = TypeVar("T", covariant=True)
C = TypeVar("C", contravariant=True)
SimpyProcess = Generator[Event, simpy.events.ConditionValue, T]

Request = NewType("Request", Event)


class DHTTimeoutError(Exception):
    """Error raised when a request times out"""

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

    _exponentials: Dict[int, Iterator[float]] = field(
        repr=False, init=False, default_factory=dict
    )
    _normals: Dict[Tuple[int, int, int], Iterator[float]] = field(
        repr=False, init=False, default_factory=dict
    )
    _choices: Dict[int, Iterator[int]] = field(
        repr=False, init=False, default_factory=dict
    )
    _rng: np.random.Generator = field(init=False, repr=False)
    seed: int = 420
    precision: int = 4
    BATCH_SIZE = 10000
    _instance = None

    def __post_init__(self):
        self.precision = 10**self.precision
        self._rng = np.random.default_rng(self.seed)

    def get_exponential(self, mean: float) -> float:
        """Draw a number from an exponential with the given mean"""
        # use ints as keys
        mean = round(mean * self.precision)
        if mean not in self._exponentials:
            self._exponentials[mean] = iter(np.ndarray(0))
        try:
            sample = next(self._exponentials[mean])
        except StopIteration:
            exp = self._rng.exponential(
                mean / self.precision, RandomBatchGenerator.BATCH_SIZE
            )
            self._exponentials[mean] = iter(exp)
            sample = next(self._exponentials[mean])
        return sample
        # return exponential

    def get_normal(self, mean: float, std_dev: float, min_cap: float) -> float:
        """Draw a number from a normal distribution given mean and stddev.
        Cap the value to a lower bound min_cap."""
        # use ints as keys
        mean = round(mean * self.precision)
        std_dev = round(std_dev * self.precision)
        min_cap = round(min_cap * self.precision)
        if not (mean, std_dev, min_cap) in self._normals:
            self._normals[(mean, std_dev, min_cap)] = iter(np.ndarray(0))
        try:
            sample = next(self._normals[(mean, std_dev, min_cap)])
        except StopIteration:
            norm = self._rng.normal(
                mean / self.precision,
                std_dev / self.precision,
                RandomBatchGenerator.BATCH_SIZE,
            )
            norm = norm[norm >= min_cap / self.precision]
            self._normals[(mean, std_dev, min_cap)] = iter(norm)
            sample = next(self._normals[(mean, std_dev, min_cap)])
        return sample

    def get_from_range(self, n: int) -> int:
        """Draw a random item from the range(n)"""
        if n not in self._choices:
            self._choices[n] = iter(np.ndarray(0))
        try:
            choice = next(self._choices[n])
        except StopIteration:
            choices = self._rng.choice(n, RandomBatchGenerator.BATCH_SIZE)
            self._choices[n] = iter(choices)
            choice = next(self._choices[n])
        return choice

    def choose(self, nodes: List[DHTNode]) -> DHTNode:
        node: DHTNode = self._rng.choice(np.array(nodes))
        return node


@dataclass
class Loggable(ABC):
    """A class for objects that can log simulation events"""

    env: Environment = field(repr=False)
    name: str = field(init=False)

    id: int = field(init=False, repr=False)

    logger: logging.Logger = field(init=False, repr=False)
    rbg: RandomBatchGenerator = field(init=False, repr=False)

    instance_id: ClassVar[count]

    def __init_subclass__(cls, /, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls.instance_id = count()

    @abstractmethod
    def __post_init__(self) -> None:
        self.logger = logging.getLogger("logger")
        self.rbg = RandomBatchGenerator()
        instance_count = next(self.instance_id)
        self.name = f"{self.__class__.__name__}_{instance_count:05d}"

    def log(self, msg: str, level: int = logging.DEBUG) -> None:
        """Log simulation events"""
        self.logger.log(level, f"{self.env.now:5.1f} {self.name:>12s}:    {msg}")
