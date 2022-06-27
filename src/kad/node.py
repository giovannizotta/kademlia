from __future__ import annotations

import logging
from dataclasses import dataclass, field
from math import log2
from typing import Iterator, List, Sequence, Set

from simpy.events import Process

from common.client import Client
from common.node import DHTNode
from common.packet import Message, MessageType, Packet
from common.utils import DHTTimeoutError, Request, SimpyProcess


@dataclass
class KadNode(DHTNode):
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=5)

    def __hash__(self):
        return self.id

    def __post_init__(self) -> None:
        super().__post_init__()
        self.buckets = [[] for _ in range(self.log_world_size)]

    def process_sender(self: KadNode, packet: Packet) -> None:
        if isinstance(packet.sender, Client):
            return
        sender = packet.sender
        assert isinstance(sender, KadNode)
        self.log(f"processing sender {sender.name}")
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

    def find_node(self, key: int | str) -> SimpyProcess[List[Process]]:
        key_hash = self._compute_key(key) if isinstance(key, str) else key
        self.log(f"Looking for key {key}")
        contacted = set()
        contacted.add(self)
        current = self.pick_neighbors(key_hash)
        assert len(current) > 0
        found = False
        hop = 0
        while not found:
            requests = self.ask_neighbors(current, contacted, key_hash)
            hop += 1
            packets: List[Packet] = list()
            try:
                yield from self.wait_resps(requests, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error", level=logging.WARNING)

            self.log(f"Received {packets}")
            found = self.update_candidates(packets, key_hash, current, contacted)

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
        self, current: List[KadNode], contacted: Set[KadNode], key: int
    ) -> List[Request]:
        # find nodes to contact
        to_contact = list()
        for node in current:
            if node not in contacted:
                contacted.add(node)
                to_contact.append(node)
            if len(to_contact) == self.alpha:
                break
        # send them a request
        # print(f"{self.name} Contacting {len(to_contact)} nodes")
        self.log(f"Contacting {to_contact} ")
        requests = list()
        for node in to_contact:
            packet = Message(ptype=MessageType.GET_NODE, data=dict(key=key))
            sent_req = self.send_req(node, packet)
            requests.append(sent_req)
        return requests

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


def neigh_picker(node: KadNode, key: int) -> Iterator[KadNode]:
    i = node.get_bucket_for(key)
    cb_iter = iter(node.buckets[i])
    lb_iter = iter(reversed(node.buckets[:i]))
    rb_iter = iter(node.buckets[i + 1 :])
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
