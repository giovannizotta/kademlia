from __future__ import annotations
from common.rbg import RandomBatchGenerator as RBG
from common.utils import *
from abc import ABC, abstractmethod


class Node(ABC):
    @abstractmethod
    def __init__(
        self,
        env: simpy.Environment,
        _id: int,
        serve_mean: float = 0.8,
        timeout: int = 5,
        neigh: Node = None,
    ):
        self.env = env
        self.id = _id
        self.serve_mean = serve_mean
        self.timeout = timeout
        self.rbg = RBG()
        self.neigh = neigh
        self.in_queue = simpy.Resource(env, capacity=1)
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
        # wait for response or timeout
        transmission_time = self.rbg.get_exponential(self.serve_mean)
        propagation_delay = self.env.timeout(transmission_time)
        timeout = self.env.timeout(self.timeout)
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
                # serve request
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
