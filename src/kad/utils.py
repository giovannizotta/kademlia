from typing import *
import simpy

# generic from hashable type
HashableT = TypeVar("HashableT", bound=Hashable)
# return type for simpy processes
SimpyProcess = Generator[simpy.Event, simpy.Event, None]
