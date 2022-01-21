from __future__ import annotations
from common.rbg import RandomBatchGenerator as RBG
from common.utils import *
from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
import hashlib
from bitstring import BitArray


@dataclass
class Node(ABC):
    env: simpy.Environment
    name: str
    serve_mean: float = 0.8
    timeout: int = 5
    log_world_size: int = 10

    @abstractmethod
    def __post_init__(self):
        self.id = Node._compute_key(self.name, self.log_world_size)
        self.rbg = RBG()
        self.dict: Dict[int, Any] = dict()
        self.in_queue = simpy.Resource(self.env, capacity=1)

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
            self.log(
                f"receive packet {packet} queue size {len(self.in_queue.queue)}")
            with self.in_queue.request() as req:
                yield req
                # serve request
                yield from succ_callback(packet, recv_req, **succ_args)
            self.log(f"Served packet {packet}, {len(self.in_queue.queue)}")

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
        self.log(
            f"receive packet {packet} queue size {len(self.in_queue.queue)}")
        with self.in_queue.request() as req:
            yield req
            yield from callback(packet, recv_req, **callback_args)

    @abstractmethod
    def on_find_node_request(
            self,
            packet: int,
            recv_req: simpy.Event,
            key: int):
        pass

    @abstractmethod
    def _on_find_node_response(
        self,
        packet: int,
        recv_req: simpy.Event,
        from_node: Node
    ) -> SimpyProcess:
        pass

    @abstractmethod
    def _on_find_node_timeout(self):
        pass

    @staticmethod
    def _compute_key(key_str: str, log_world_size: int) -> int:
        digest = hashlib.sha256(bytes(key_str, "utf-8")).hexdigest()
        bindigest = BitArray(hex=digest).bin
        subbin = bindigest[:log_world_size]
        return BitArray(bin=subbin).uint

    @abstractclassmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        pass

    # to implement:
    # join, leave, (crash ?), store_value
