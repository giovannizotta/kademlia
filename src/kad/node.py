from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from math import log2

from common.node import DHTNode, Packet, PacketType, Request
from common.utils import *


def decide_value(packets: List[Packet]):
    # give the most popular value
    d = defaultdict(int)
    for packet in packets:
        if d[packet.data["value"]] is not None:
            d[packet.data["value"]] += 1
    if d:
        return max(d, key=lambda k: d[k])
    else:
        return None


@dataclass
class KadNode(DHTNode):
    neigh: Optional[KadNode] = None
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    capacity: int = field(repr=False, default=100)
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=5)

    def __hash__(self):
        return self.id

    def __post_init__(self) -> None:
        super().__post_init__()
        self.max_timeout = 10
        self.buckets = [[] for _ in range(self.log_world_size)]

    def process_sender(self: KadNode, packet: Packet) -> None:
        sender = cast(KadNode, packet.sender)
        self.log(f"processing sender {sender.name}")
        self.update_bucket(sender)

    def manage_packet(self, packet: Packet):
        self.process_sender(packet)
        super().manage_packet(packet)

    def get_node(self, packet: Packet) -> None:
        neighs = self.pick_neighbors(packet.data["key"])
        new_packet = Packet(ptype=PacketType.GET_NODE_REPLY,
                            data=dict(neighbors=neighs), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def update_candidates(self, packets: Sequence[Packet], key: int, current: List[KadNode], contacted: Set[KadNode]) -> bool:
        current_set = set(current)
        for packet in packets:
            for neigh in packet.data["neighbors"]:
                current_set.add(neigh)
        # print(current_set)

        candidates = sorted(
            current_set, key=lambda x: x._compute_distance(key))
        candidates = candidates[:self.k]
        if candidates != current and not all(c in contacted for c in candidates):
            found = False
            current.clear()
            for c in candidates:
                current.append(c)
        else:
            found = True
        return found

    def ask(self, to: List[KadNode], packet: Packet, type: PacketType) -> List[Request]:
        requests = []
        for node in to:
            self.log(
                f"asking {type} to {node} for the key {packet.data['key']} {packet.data.get('value', '')}")
            new_packet = Packet(ptype=type, data=packet.data)
            sent_req = self.send_req(packet.sender, new_packet)
            requests.append(sent_req)
        return requests

    def find_value(self, packet: Packet) -> SimpyProcess[None]:
        key = packet.data["key"]
        packets = []
        nodes, hops = yield from self.find_node(key)

        try:
            requests = self.ask(nodes, packet, PacketType.GET_VALUE)
            yield from self.wait_resps(requests, packets)
        except DHTTimeoutError:
            if not packets:
                hops = -1

        value = decide_value(packets)
        new_packet = Packet(ptype=PacketType.FIND_VALUE_REPLY, data=dict(
            value=value, hops=hops), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def store_value(self, packet: Packet) -> SimpyProcess[None]:
        key = packet.data["key"]
        nodes, hops = yield from self.find_node(key)
        try:
            requests = self.ask(nodes, packet, PacketType.SET_VALUE)
            yield from self.wait_resps(requests, [])
        except DHTTimeoutError:
            hops = -1

        new_packet = Packet(ptype=PacketType.STORE_VALUE_REPLY,
                            data=dict(hops=hops), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def get_bucket_for(self, key: int) -> int:
        dst = self._compute_distance(key)
        return dst if dst == 0 else int(log2(dst))

    def update_bucket(self, node: KadNode) -> None:
        if node is self:
            return

        self.log(f"Updating buckets with node {node.name}")
        bucket = self.buckets[self.get_bucket_for(node.id)]
        try:
            index = bucket.index(node)
            for i in range(index, len(bucket) - 1):
                bucket[i] = bucket[i+1]
            bucket[-1] = node
        except ValueError:
            if len(bucket) < self.k:
                bucket.append(node)
            else:
                for i in range(len(bucket) - 1):
                    bucket[i] = bucket[i+1]
                bucket[-1] = node

    def find_node(self, key: int) -> SimpyProcess[Tuple[List[KadNode], int]]:
        self.log(f"Looking for key {key}")
        contacted = set()
        contacted.add(self)
        current = self.pick_neighbors(key)
        assert len(current) > 0
        found = False
        hop = 0
        old_current = list()
        while not found:
            requests = self.ask_neighbors(current, contacted, key)
            hop += 1
            assert requests, f"{hop}, {current}, {old_current}, {contacted}"
            packets: List[Packet] = []
            try:
                yield from self.wait_resps(requests, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error", level=logging.WARNING)

            old_current = current.copy()
            # print(f"Received {packets}")
            found = self.update_candidates(packets, key, current, contacted)

        for node in current:
            self.update_bucket(node)
        return current, hop

    def pick_neighbors(self, key: int) -> List[KadNode]:
        nodes = set()
        for neigh in neigh_picker(self, key):
            nodes.add(neigh)
            if len(nodes) == self.k:
                break
        return sorted(nodes, key=lambda x: x._compute_distance(key))[:self.k]

    def ask_neighbors(self, current: List[KadNode], contacted: Set[KadNode], key: int) -> List[Request]:
        # find nodes to contact
        to_contact = []
        for node in current:
            if node not in contacted:
                contacted.add(node)
                to_contact.append(node)
            if len(to_contact) == self.alpha:
                break
        # send them a request
        # print(f"{self.name} Contacting {len(to_contact)} nodes")
        self.log(f"Contacting {to_contact} ")
        requests = []
        for node in to_contact:
            packet = Packet(ptype=PacketType.GET_NODE, data=dict(key=key))
            sent_req = self.send_req(node, packet)
            requests.append(sent_req)
        return requests

    def _compute_distance(self, from_key: int) -> int:
        return self.id ^ from_key

    def join_network(self, to: DHTNode) -> SimpyProcess[None]:
        to = cast(KadNode, to)
        self.update_bucket(to)
        yield from self.find_node(self.id)
        self.log(f"joined the network")


def neigh_picker(node: KadNode, key: int) -> Iterator[KadNode]:
    i = node.get_bucket_for(key)
    cb_iter = iter(node.buckets[i])
    lb_iter = iter(reversed(node.buckets[:i]))
    rb_iter = iter(node.buckets[i+1:])
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
