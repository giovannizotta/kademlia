from common.utils import *
from common.node import Node


class KadNode(Node):
    def __init__(
        self,
        env: simpy.Environment,
        _id: int,
        serve_mean: float = 0.8,
        timeout: int = 5,
        neigh: Node = None,
    ):
        super().__init__(env, _id, serve_mean, timeout, neigh)

    def get_key_request(
            self,
            packet: int,
            recv_req: simpy.Event,
            key: int,
            forward: bool) -> SimpyProcess:
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
                    next_node.get_key_request,
                    dict(key=key, forward=False)
                )
            )

            self.env.process(
                self.wait_response(
                    packet,
                    sent_req,
                    recv_req,
                    self.get_key_response,
                    dict(from_node=next_node),
                    self.get_key_timeout,
                    dict()
                )
            )
        else:
            self.log(f"packet {packet} -> key {key} found")
            recv_req.succeed()

    def get_key_response(
            self,
            packet: int,
            sent_req: simpy.Event,
            recv_req: simpy.Event,
            from_node: Node) -> SimpyProcess:
        """Manage the response of a get key request"""
        # wait some time
        service_time = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(service_time)
        self.log(f"serve answer for packet {packet} from node {from_node.id}")
        # answer with the received key
        recv_req.succeed()

    def get_key_timeout(self) -> SimpyProcess:
        # do nothing at the moment
        raise StopIteration
