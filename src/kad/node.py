from __future__ import annotations
from common.utils import *
from common.node import Node
from dataclasses import dataclass


@dataclass
class KadNode(Node):
    neigh: Optional[KadNode] = None

    def __post_init__(self):
        super().__post_init__()

    def find_node(self, key: int) -> SimpyProcess:
        return super().find_node(key)

    def on_find_node_request(
        self,
        packet: int,
        recv_req: simpy.Event,
        key: int,
        forward: bool
    ) -> SimpyProcess:
        """Serve a request for the given packet"""

        self.log(f"serving packet {packet} -> request {key}")
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)

        if forward:
            next_node = self.neigh
            sent_req = simpy.Event(self.env)
            self.env.process(
                next_node.wait_request(
                    packet,
                    sent_req,
                    next_node.on_find_node_request,
                    dict(key=key, forward=False)
                )
            )

            self.env.process(
                self.wait_response(
                    packet,
                    sent_req,
                    recv_req,
                    self._on_find_node_response,
                    dict(from_node=next_node),
                    self._on_find_node_timeout,
                    dict()
                )
            )
        else:
            self.log(f"packet {packet} -> key {key} found")
            recv_req.succeed()

    def _on_find_node_response(
        self,
        packet: int,
        sent_req: simpy.Event,
        recv_req: simpy.Event,
        from_node: Node
    ) -> SimpyProcess:
        """Manage the response of a get key request"""
        # wait some time
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        self.log(f"serve answer for packet {packet} from node {from_node.id}")
        # answer with the received key
        recv_req.succeed()

    def _on_find_node_timeout(self) -> SimpyProcess:
        # do nothing at the moment
        raise StopIteration

    def join_network(self, from_node: Node) -> SimpyProcess:
        return super().join_network(from_node)

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        # keys are log_world_size bits long
        return key1 ^ key2