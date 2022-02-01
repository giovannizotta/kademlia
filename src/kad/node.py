from __future__ import annotations
from concurrent.futures import process
from common.utils import *
from common.node import DHTNode, packet_service, Packet, Request
from math import log2
from dataclasses import dataclass
from collections.abc import Sequence


def get_bucket_for(key: int) -> int:
    return int(log2(key))


def process_sender(operation: Callable[..., T]) -> Callable[..., T]:
    def wrapper(self: KadNode, packets: Union[Packet, Sequence[Packet]], *args: Any) -> T:
        if not isinstance(packets, Sequence):
            packets = [packets]
        for packet in packets:
            sender = cast(KadNode, packet.sender)
            bucket = self.buckets[get_bucket_for(sender.id)]
            try:
                index = bucket.index(sender)
                for i in range(index, len(bucket) - 1):
                    bucket[i] = bucket[i+1]
                bucket[-1] = sender
            except ValueError:
                if len(bucket) < self.k:
                    bucket.append(sender)
                else:
                    pass  # TODO: ping least recently seen node and swap with the new one if needed
        return operation(self, packets, *args)

    return wrapper


@dataclass
class KadNode(DHTNode):
    neigh: Optional[KadNode] = None
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=20)

    def __post_init__(self):
        super().__post_init__()
        self.buckets = [[] for _ in range(self.log_world_size)]

    def find_node(self, key: int) -> SimpyProcess[List[KadNode]]:
        contacted = set()
        contacted.add(self)
        current = yield from self.find_neighbors(key, self.k)
        found = False
        while not found:
            requests = yield from self.ask_neighbors(contacted, key, self.k)
            packets: List[Packet] = []
            try:
                yield from self.wait_resps(requests, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error")

            found = yield from self.update_candidates(packets, key, current)

        return current

    @packet_service
    @process_sender
    def update_candidates(self, packets: Sequence[Packet], key: int, current: List[KadNode]) -> bool:
        current_set = set(current)
        for packet in packets:
            for neigh in packet.data["neighbors"]:
                current_set.add(neigh)

        candidates = sorted(current_set, key=lambda x: KadNode._compute_distance(
            x.id, key, self.log_world_size))
        candidates = candidates[:self.k]
        if candidates != current:
            found = False
            current.clear()
            for c in candidates:
                current.append(c)
        else:
            found = True
        return found

    @packet_service
    def find_neighbors(self, key: int, k: int) -> List[KadNode]:
        nodes = set()
        for neigh in NeighborPicker(self, key):
            nodes.add(neigh)
            if len(nodes) == k:
                break
        return sorted(nodes, key=lambda x: KadNode._compute_distance(x.id, key, self.log_world_size))

    @packet_service
    @process_sender
    def get_neighbors(self, packet: Packet, recv_req: Request) -> None:
        neighs = self.find_neighbors(packet.data["key"], packet.data["k"])
        packet.data["neighbors"] = neighs
        self.send_resp(recv_req, packet)

    @packet_service
    def ask_neighbors(self, current: List[KadNode], contacted: Set[KadNode], key: int, k: int) -> List[Request]:
        # find nodes to contact
        to_contact = []
        for node in current:
            if node not in contacted:
                contacted.add(node)
                to_contact.append(node)
            if len(to_contact) == self.alpha:
                break
        # send them a request
        requests = []
        for node in to_contact:
            packet = Packet()
            packet.data["key"] = key
            packet.data["k"] = k
            sent_req = self.send_req(node.get_neighbors, packet)
            requests.append(sent_req)
        return requests

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        # keys are log_world_size bits long
        return key1 ^ key2


class NeighborPicker:
    def __init__(self, node, key):
        index = get_bucket_for(key)
        self.current_nodes = node.buckets[index]
        self.left_buckets = node.buckets[:index]
        self.right_buckets = node.buckets[(index + 1):]
        self.left = True

    def __iter__(self):
        return self

    def __next__(self):
        """
        Pop an item from the left subtree, then right, then left, etc.
        """
        if self.current_nodes:
            return self.current_nodes.pop()

        if self.left and self.left_buckets:
            self.current_nodes = self.left_buckets.pop()
            self.left = False
            return next(self)

        if self.right_buckets:
            self.current_nodes = self.right_buckets.pop(0)
            self.left = True
            return next(self)

        raise StopIteration
