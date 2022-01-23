from __future__ import annotations
from common.utils import *
from common.node import Node
from dataclasses import dataclass, field
from common.packet import Packet


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

    def update(self) -> SimpyProcess:
        for x in range(self.log_world_size):
            key = (self.id * 2**x) % (2 ** self.log_world_size)
            node = yield from self.find_node(key)
            self.ft[x] = node

    def find_node(self, key):
        packet = Packet()
        sent_req = simpy.Event(self.env)
        self.env.process(
            self.wait_request(
                packet,
                sent_req,
                self.find_node_request,
                dict(key = key)
            )
        )
        yield sent_req
        return packet.data["best_node"]

    def forward_find_node_request(
        self,
        packet: Packet,
        key: int,
        best_node: Node,
        recv_req: simpy.Event
    ) -> None:
        """Forward FIND_NODE request to best_node"""
        self.log(f"Forwarding {key} request to {best_node.id}")

        sent_req = simpy.Event(self.env)
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

    def find_node_request(
        self,
        packet: Packet,
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
            packet.data["best_node"] = self
            recv_req.succeed()
        else:
            self.forward_find_node_request(packet, key, best_node, recv_req)

    def on_find_node_request(
        self,
        packet: Packet,
        recv_req: simpy.Event,
        key: int
    ) -> SimpyProcess:

        best_node = min(self.ft, key=lambda node: ChordNode._compute_distance(
            node.id, key, self.log_world_size))

        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)

        packet.data["best_node"] = best_node
        recv_req.succeed()

    def _on_find_node_response(
        self,
        packet: Packet,
        sent_req: simpy.Event,
        recv_req: simpy.Event,
        from_node: Node,
        key: int
    ) -> SimpyProcess:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        best_node = packet.data["best_node"]
        if best_node is from_node:
            self.log(f"Found {key}, it's {best_node.id}")
            recv_req.succeed()
        else:
            self.log(f"Forwarding {key} to {best_node.id}")
            self.forward_find_node_request(packet, key, best_node, recv_req)

    def _on_find_node_timeout(self):
        return super()._on_find_node_timeout()


    def get_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        packet.data["succ"] = self.succ
        recv_req.succeed()

    def set_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        self.succ = packet.data["succ"]
        recv_req.succeed()

    def get_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        packet.data["pred"] = self.pred
        recv_req.succeed()

    def set_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        self.pred = packet.data["pred"]
        recv_req.succeed()

    def ask_successor(self, to: Node) -> SimpyProcess[Node]:
        packet = Packet()
        sent_req = simpy.Event(self.env)
        recv_req = simpy.Event(self.env)
        self.env.process(
            to.wait_request(
                packet,
                sent_req,
                to.get_successor,
                dict()
            )
        )
        # ... and wait for an answer
        yield self.env.process(
            self.wait_response(
                packet,
                sent_req,
                recv_req,
                self.do_nothing,
                dict(),
                self._on_find_node_timeout,
                dict()
            )
        )

        return packet.data["succ"]

    def join_network(self, to: Node) -> SimpyProcess:
        packet = Packet()
        sent_req = simpy.Event(self.env)
        recv_req = simpy.Event(self.env)
        # forward request...
        self.env.process(
            to.wait_request(
                packet,
                sent_req,
                to.find_node_request,
                dict(key=self.id)
            )
        )
        # ... and wait for an answer
        yield self.env.process(
            self.wait_response(
                packet,
                sent_req,
                recv_req,
                self.do_nothing,
                dict(),
                self._on_find_node_timeout,
                dict()
            )
        )
        node = packet.data["best_node"]
        succ = yield from self.ask_successor(node)

        # ask node to set successor as me
        sent_req = simpy.Event(self.env)
        recv_req = simpy.Event(self.env)
        packet.data["succ"] = self 
        self.env.process(
            node.wait_request(
                packet,
                sent_req,
                node.set_successor,
                dict()
            )
        )
        # ... and wait for an answer
        yield self.env.process(
            self.wait_response(
                packet,
                sent_req,
                recv_req,
                self.do_nothing,
                dict(),
                self._on_find_node_timeout,
                dict()
            )
        )

        # ask succ to set predecessor as me
        sent_req = simpy.Event(self.env)
        recv_req = simpy.Event(self.env)
        packet.data["pred"] = self
        self.env.process(
            succ.wait_request(
                packet,
                sent_req,
                succ.set_predecessor,
                dict()
            )
        )
        # ... and wait for an answer
        yield self.env.process(
            self.wait_response(
                packet,
                sent_req,
                recv_req,
                self.do_nothing,
                dict(),
                self._on_find_node_timeout,
                dict()
            )
        )

        #I do my rewiring
        self.pred = node
        self.succ = succ

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        """Compute the distance from key1 to key 2"""
        return (key2 - key1) % (2**log_world_size - 1)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
