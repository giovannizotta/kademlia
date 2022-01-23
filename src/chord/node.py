from __future__ import annotations
from common.utils import *
from common.node import Node
from dataclasses import dataclass, field


@dataclass
class ChordNode(Node):
    _pred: Optional[ChordNode] = field(init=False, repr=False)
    _succ: Optional[ChordNode] = field(init=False, repr=False)
    ft: List[ChordNode] = field(init=False, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ft: List[ChordNode] = [self] * self.log_world_size

    @property
    def succ(self) -> ChordNode:
        return self._succ

    @succ.setter
    def succ(self, node: ChordNode) -> None:
        self._succ = node
        self.ft[1] = node

    @property
    def pred(self) -> None:
        return self._pred

    @pred.setter
    def pred(self, node: ChordNode) -> None:
        self._pred = node

    def forward_find_node(
        self,
        packet: int,
        key: int,
        best_node: Node,
        recv_req: simpy.Event
    ) -> None:
        """Forward FIND_NODE request to best_node"""
        self.log(f"Forwarding {key} request to {best_node.id}")

        sent_req = simpy.Event(self.env)
        packet = []
        # forward request...
        self.env.process(
            best_node.wait_request(
                packet,
                sent_req,
                best_node.on_find_node_request,
                dict(key=key)
            )
        )
        # ... and wait for an answer
        self.env.process(
            self.wait_response(
                packet,
                sent_req,
                recv_req,
                self._on_find_node_response,
                dict(from_node=best_node,
                     key=key),
                self._on_find_node_timeout,
                dict()
            )
        )

    def find_node(
        self,
        packet: int,
        recv_req: simpy.Event,
        key: int
    ) -> SimpyProcess:

        self.log(f"Looking for node with {key}")

        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)

        best_node = min(self.ft, key=lambda node: ChordNode._compute_distance(
            node.id, key, self.log_world_size))

        if best_node is self:
            self.log(f"Found {key}, it's me")
            recv_req.succeed()
        else:
            self.forward_find_node(packet, key, best_node, recv_req)

    def on_find_node_request(
        self,
        packet: List[ChordNode],
        recv_req: simpy.Event,
        key: int
    ) -> SimpyProcess:

        best_node = min(self.ft, key=lambda node: ChordNode._compute_distance(
            node.id, key, self.log_world_size))

        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)

        packet.append(best_node)
        recv_req.succeed()

    def _on_find_node_response(
        self,
        packet: List[ChordNode],
        sent_req: simpy.Event,
        recv_req: simpy.Event,
        from_node: Node,
        key: int
    ) -> SimpyProcess[ChordNode]:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        best_node = packet.pop()
        if best_node is from_node:
            self.log(f"Found {key}, it's {best_node.id}")
            recv_req.succeed()
        else:
            self.log(f"Forwarding {key} to {best_node.id}")
            self.forward_find_node(packet, key, best_node, recv_req)

    def _on_find_node_timeout(self):
        return super()._on_find_node_timeout()

    def join_network(self, to: Node) -> SimpyProcess:
        return super().join_network(to)

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        """Compute the distance from key1 to key 2"""
        return (key2 - key1) % (2**log_world_size - 1)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
