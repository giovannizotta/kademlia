from __future__ import annotations
from typing import *
import simpy

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
