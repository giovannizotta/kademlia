from __future__ import annotations
from common.utils import *
from common.node import Node
from dataclasses import dataclass

@dataclass
class ChordNode(Node):
    pred: Optional[ChordNode] = None
    succ: Optional[ChordNode] = None

    def __post_init__(self):
        super().__post_init__()
        self.ft: List[ChordNode] = list()

    def on_find_node_request(self, packet: int, recv_req: simpy.Event, key: int):
        super().on_find_node_request(packet, recv_req, key)

    def _on_find_node_response(self, packet: int, recv_req: simpy.Event, from_node: Node) -> SimpyProcess:
        return super()._on_find_node_response(packet, recv_req, from_node)

    def _on_find_node_timeout(self):
        return super()._on_find_node_timeout()

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        """Compute the distance from key1 to key 2"""
        return (key2 - key1) % (2**log_world_size - 1)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date 
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)


