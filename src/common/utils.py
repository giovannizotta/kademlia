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

import json
import numpy as np
import simpy
import simpy.events
from math import radians, cos, sin, asin, sqrt
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
class LocationManager(metaclass=Singleton):
    filename: str = field(default="src/common/bitcoin_nodes.json")
    iters: int = field(init=False, default = 0)
    location_list: List[Tuple[float, float]] = field(repr=False, init=False, default_factory=list)
    _it: Iterator[Tuple[float, float]] = field(repr=False, init=False)

    def __post_init__(self):
        self.rbg = RandomBatchGenerator()
        self._parse_bitcoin_nodes()
        self.rbg.shuffle(self.location_list)
        self._it = iter(self.location_list)

    def _parse_bitcoin_nodes(self):
        with open(self.filename, "r", encoding="utf8") as f:
            data = json.loads(f.read())
            for node in data["nodes"].values():
                if node[8] is not None and node[8] > 0:
                    self.location_list.append((node[8], node[9]))

    def get(self):
        self.iters+=1
        try:
            return next(self._it)
        except StopIteration:
            self._it = iter(self.location_list)
            return next(self._it)

    def distance(self, point1: Tuple[float, float], point2: Tuple[float, float]):
        # from https://www.geeksforgeeks.org/program-distance-two-points-earth/
        lat1, lon1 = point1
        lat2, lon2 = point2
        # The math module contains a function named
        # radians which converts from degrees to radians.
        lon1 = radians(lon1)
        lon2 = radians(lon2)
        lat1 = radians(lat1)
        lat2 = radians(lat2)
          
        # Haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
     
        c = 2 * asin(sqrt(a))
        
        # Radius of earth in kilometers. 
        r = 6371
          
        # calculate the result
        return int(c * r)


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

    def shuffle(self, l: list) -> None:
        self._rng.shuffle(l)

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
