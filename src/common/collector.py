from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class DataCollector:
    """Collect the data from the simulation"""

    # list of times when there was a timeout
    timed_out_requests: List[float] = field(default_factory=list)
    # tuple (time, latency, hops)
    client_requests: List[Tuple[float, float, int]] = field(default_factory=list)
    # dict from node id to tuple (time, load)
    queue_load: Dict[str, List[Tuple[float, int]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # dict from node id to time of join
    joined_time: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # dict from node id to time of crash
    crashed_time: Dict[str, float] = field(default_factory=lambda: defaultdict(float))

    def clear(self) -> None:
        self.timed_out_requests = 0
        self.client_requests.clear()
        self.queue_load.clear()
        self.crashed_time.clear()

    def to_dict(self) -> Dict[str, any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, dct) -> DataCollector:
        return DataCollector(**dct)
