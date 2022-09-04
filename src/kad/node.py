from __future__ import annotations

import logging
from dataclasses import dataclass, field
from math import log2
from typing import Iterator, List, Sequence, Set, Union, Tuple

from simpy.events import Process

from common.client import Client
from common.node import DHTNode
from common.packet import Message, MessageType, Packet
from common.utils import DHTTimeoutError, Request, SimpyProcess
from simulation.constants import DEFAULT_KAD_FIND_NODE_TIMEOUT


@dataclass
class KadNode(DHTNode):
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    blackset: Set[KadNode] = field(init=False, repr=False)
    alpha: int = field(repr=False)
    k: int = field(repr=False)

    def __hash__(self):
        return self.id

    def __post_init__(self) -> None:
        super().__post_init__()
        self.buckets = [[] for _ in range(self.log_world_size)]
        self.blackset = set()

    def process_sender(self: KadNode, packet: Packet) -> None:
        if isinstance(packet.sender, Client):
            return
        sender = packet.sender
        assert isinstance(sender, KadNode)
        self.log(f"processing sender {sender.name}")
        if sender in self.blackset:
            self.blackset.remove(sender)
        self.update_bucket(sender)

    def manage_packet(self, packet: Packet) -> None:
        self.process_sender(packet)
        super().manage_packet(packet)

    def get_node(self, packet: Packet) -> None:
        msg = packet.message
        neighs = self.pick_neighbors(msg.data["key"])
        reply = Message(
            ptype=MessageType.GET_NODE_REPLY,
            data=dict(neighbors=neighs),
            event=msg.event,
        )
        self.send_resp(packet.sender, reply)

    def update_candidates(
            self,
            packets: Sequence[Packet],
            key: int,
            current: List[KadNode],
            contacted: Set[KadNode],
    ) -> bool:
        current_set = set(current)
        for packet in packets:
            for neigh in packet.message.data["neighbors"]:
                if neigh not in self.blackset:
                    current_set.add(neigh)
        # print(current_set)

        candidates = sorted(current_set, key=lambda x: x._compute_distance(key))
        candidates = candidates[: self.k]
        if candidates != current and not all(c in contacted for c in candidates):
            found = False
            current.clear()
            for c in candidates:
                current.append(c)
        else:
            found = True
        return found

    def get_bucket_for(self, key: int) -> int:
        dst = self._compute_distance(key)
        return dst if dst == 0 else int(log2(dst))

    def update_bucket(self, node: KadNode) -> None:
        if node is self:
            return

        bucket = self.buckets[self.get_bucket_for(node.id)]
        try:
            index = bucket.index(node)
            for i in range(index, len(bucket) - 1):
                bucket[i] = bucket[i + 1]
            bucket[-1] = node
        except ValueError:
            if len(bucket) < self.k:
                bucket.append(node)
            else:
                for i in range(len(bucket) - 1):
                    bucket[i] = bucket[i + 1]
                bucket[-1] = node

    def remove_from_bucket(self, node: KadNode) -> None:
        bucket = self.buckets[self.get_bucket_for(node.id)]
        bucket.remove(node)
        self.blackset.add(node)

    def find_strict_parallelism(self, key_hash: int, current: List[KadNode], contacted: Set[KadNode]) -> \
            Tuple[List[KadNode], int]:
        found = False
        hop = 0
        while not found:
            to_contact, requests = self.ask_neighbors(current, contacted, key_hash)
            hop += 1
            packets: List[Packet] = list()
            try:
                yield from self.wait_resps(requests, packets, DEFAULT_KAD_FIND_NODE_TIMEOUT)
                self.clear_timeout_nodes(to_contact, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error", level=logging.WARNING)

            self.log(f"Received {packets}")
            found = self.update_candidates(packets, key_hash, current, contacted)
        return current, hop

    def find_bounded_parallelism(self, key_hash: int, current: List[KadNode], contacted: Set[KadNode]) -> \
            Tuple[List[KadNode], int]:
        finished = False
        hop = 0
        active_requests = set()
        while not finished:
            _, requests = self.ask_neighbors(current, contacted, key_hash, len(active_requests))
            req_or_timeout = [req | self.env.timeout(DEFAULT_KAD_FIND_NODE_TIMEOUT) for req in requests]
            active_requests.update(req_or_timeout)
            hop += 1
            try:
                packet = yield from self.wait_any_resp(active_requests)
                self.log(f"Received {packet}")
                updated = self.update_candidates([packet], key_hash, current, contacted)
                finished = updated and not active_requests
            except DHTTimeoutError:
                self.log("DHT timeout error", level=logging.WARNING)

        return current, hop

    def find_node(self, key: Union[int, str]) -> SimpyProcess[List[Process]]:
        key_hash = self._compute_key(key) if isinstance(key, str) else key
        self.log(f"Looking for key {key}")
        contacted = set()
        contacted.add(self)
        current = self.pick_neighbors(key_hash)
        assert len(current) > 0
        current, hop = yield from self.find_bounded_parallelism(key_hash, current, contacted)

        for node in current:
            self.update_bucket(node)
        processes = []
        self.log(f"finished find_node, nodes: {current}")
        for node in current:
            def p(n, h):
                yield self.env.timeout(0)
                return n, h

            processes.append(self.env.process(p(node, hop)))
        self.log(f"processes: {processes}")
        return processes

    def pick_neighbors(self, key: int) -> List[KadNode]:
        nodes = set()
        for neigh in neigh_picker(self, key):
            nodes.add(neigh)
            if len(nodes) == self.k:
                break
        return sorted(nodes, key=lambda x: x._compute_distance(key))[: self.k]

    def ask_neighbors(
            self, current: List[KadNode], contacted: Set[KadNode], key: int, num_active_requests: int = 0,
    ) -> Tuple[List[KadNode], List[Request]]:
        num_to_contact = self.alpha - num_active_requests
        # find nodes to contact
        to_contact = list()
        for node in current:
            if node not in contacted:
                contacted.add(node)
                to_contact.append(node)
            if len(to_contact) == num_to_contact:
                break
        # send them a request
        # print(f"{self.name} Contacting {len(to_contact)} nodes")
        self.log(f"Contacting {to_contact} ")
        requests = list()
        for node in to_contact:
            packet = Message(ptype=MessageType.GET_NODE, data=dict(key=key))
            sent_req = self.send_req(node, packet)
            requests.append(sent_req)
        return to_contact, requests

    def _compute_distance(self, from_key: int) -> int:
        return self.id ^ from_key

    def join_network(self, to: KadNode) -> SimpyProcess[bool]:
        self.update_bucket(to)
        nodes, _ = yield from self.unzip_find(self.id, self.env.all_of)
        if len(nodes) > 0:
            self.log("Joined the network")
            return True
        self.log("Not able to join the network", level=logging.WARNING)
        return False

    def clear_timeout_nodes(self, to_contact: List[KadNode], packets: List[Packet]) -> None:
        answering = {packet.sender for packet in packets}
        for node in to_contact:
            if node not in answering:
                self.remove_from_bucket(node)

    def wait_any_resp(self, active_requests: Set[Request]) -> SimpyProcess[Packet]:
        ans = yield self.env.any_of(active_requests)
        assert len(ans) == 1
        for event, ret_val in ans.items():
            active_requests.remove(event)
            if isinstance(ret_val, Packet):
                return ret_val
            else:
                raise DHTTimeoutError()


def neigh_picker(node: KadNode, key: int) -> Iterator[KadNode]:
    i = node.get_bucket_for(key)
    cb_iter = iter(node.buckets[i])
    lb_iter = iter(reversed(node.buckets[:i]))
    rb_iter = iter(node.buckets[i + 1:])
    left = True

    while True:
        try:
            yield next(cb_iter)
        except StopIteration:
            if left:
                i1, i2 = lb_iter, rb_iter
            else:
                i1, i2 = rb_iter, lb_iter
            left = not left
            try:
                cb_iter = iter(next(i1))
            except StopIteration:
                try:
                    cb_iter = iter(next(i2))
                except StopIteration:
                    return
