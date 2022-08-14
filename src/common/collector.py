from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Union

import pandas as pd


@dataclass
class DataCollector:
    """Collect the data from the simulation"""

    # list of times when there was a timeout
    timed_out_requests: List[int] = field(default_factory=list)
    # tuple (time, latency, hops)
    client_requests: List[Tuple[int, float, int]] = field(default_factory=list)
    # dict from node id to tuple (time, load)
    queue_load: Dict[str, List[Tuple[int, int]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # dict from node id to time of join
    joined_time: Dict[str, int] = field(default_factory=lambda: defaultdict(float))
    # dict from node id to time of crash
    crashed_time: Dict[str, int] = field(default_factory=lambda: defaultdict(float))
    # dict from node id to list timestamps of messages received after crash
    messages_after_crash: Dict[str, List[int]] = field(default_factory=lambda: defaultdict(list))

    def clear(self) -> None:
        self.client_requests.clear()
        self.queue_load.clear()
        self.crashed_time.clear()

    def to_dict(self) -> Dict[str, any]:
        return self.__dict__

    @classmethod
    def from_dict(cls, dct) -> DataCollector:
        return DataCollector(**dct)

    def to_pandas(self) -> Dict[str, Union[pd.DataFrame, pd.Series]]:
        return {
            "timed_out_requests": pd.Series(self.timed_out_requests, name="time"),
            "client_requests": pd.DataFrame(self.client_requests, columns=["time", "latency", "hops"]),
            "joined_time": pd.Series(self.joined_time, name="time"),
            "crashed_time": pd.Series(self.crashed_time, name="time"),
            "queue_load": pd.DataFrame(self.queue_load, columns=["node", "time", "load"]),
        }
