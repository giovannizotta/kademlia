from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Union, ClassVar

import pandas as pd


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
    # dict from node id to list timestamps of messages received after crash
    messages_after_crash: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))

    DECIMALS: ClassVar[int] = 2

    def clear(self) -> None:
        self.client_requests.clear()
        self.queue_load.clear()
        self.crashed_time.clear()

    def to_dict(self) -> Dict[str, any]:
        return {
            "timed_out_requests": [round(t, self.DECIMALS) for t in self.timed_out_requests],
            "client_requests": [(round(t, self.DECIMALS), round(l, self.DECIMALS), h) for t, l, h in
                                self.client_requests],
            "joined_time": {k: round(v, self.DECIMALS) for k, v in self.joined_time.items()},
            "crashed_time": {k: round(v, self.DECIMALS) for k, v in self.crashed_time.items()},
            "queue_load": {k: [(round(t, self.DECIMALS), l) for t, l in v] for k, v in
                           self.queue_load.items()},
            "messages_after_crash": {k: [round(t, self.DECIMALS) for t in v] for k, v in
                                     self.messages_after_crash.items()},
        }

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
