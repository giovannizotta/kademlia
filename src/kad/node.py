from __future__ import annotations
from kad.rbg import RandomBatchGenerator as RBG
from kad.utils import *


class Node:
    def __init__(self, env: simpy.Environment, _id: int, serve_mean: float = 0.8, neigh: Node = None):
        self.env = env
        self.id = _id
        self.serve_mean = serve_mean
        self.rbg = RBG()
        self.in_queue = simpy.Resource(env, capacity=1)
        self.neigh = neigh
        self.queue_size = 0

    def log(self, msg: str) -> None:
        print(f"{self.env.now:5.1f} Node {self.id}: {msg}")

    def wait_response(
        self,
        packet: int,
        sent_req: simpy.Event,
        recv_req: simpy.Event,
        succ_callback: SimpyProcess,
        succ_args: Dict,
        timeout_callback: SimpyProcess,
        timeout_args: Dict
    ) -> SimpyProcess:

        transmission_time = self.rbg.get_exponential(self.serve_mean)
        propagation_delay = self.env.timeout(transmission_time)
        timeout = self.env.timeout(10)
        wait_resp = sent_req & propagation_delay
        wait_event = timeout | wait_resp
        yield wait_event

        if timeout.processed:
            # manage timeout instantly
            self.log(f"TIMEOUT Request for packet {packet}")
            yield from timeout_callback(packet,
                                        sent_req,
                                        recv_req,
                                        **timeout_args)
        else:
            # wait queue
            self.log(f"receive packet {packet} queue size {self.queue_size}")
            with self.in_queue.request() as req:
                self.queue_size += 1
                yield req
                yield from succ_callback(packet, sent_req, recv_req, **succ_args)
            self.queue_size -= 1
            self.log(f"Served packet {packet}, {self.queue_size}")

    def wait_request(
        self,
        packet: int,
        recv_req: simpy.Event,
        callback: SimpyProcess,
        callback_args: Dict
    ) -> SimpyProcess:
        """Serve a request for the given packet"""
        transmission_time = self.rbg.get_exponential(self.serve_mean)
        propagation_delay = self.env.timeout(transmission_time)
        yield propagation_delay
        self.log(f"receive packet {packet} queue size {self.queue_size}")
        with self.in_queue.request() as req:
            self.queue_size += 1
            yield req
            yield from callback(packet, recv_req, **callback_args)

    def get_key_request(self, packet: int, recv_req: simpy.Event, key: int, forward: bool) -> SimpyProcess:
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
