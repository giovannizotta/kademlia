from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class DataCollector:
    """Collect the data from the simulation"""

    timed_out_requests: int = 0
    client_requests: List[Tuple[float, int]] = field(default_factory=list)
    queue_load: Dict[str, List[Tuple[float, int]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    joined_time: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
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
