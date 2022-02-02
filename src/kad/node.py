from __future__ import annotations
from concurrent.futures import process
from common.utils import *
from common.node import DHTNode, packet_service, Packet, Request
from math import log2
from dataclasses import dataclass
from collections.abc import Sequence


def process_sender(operation: Callable[..., T]) -> Callable[..., T]:
    def wrapper(self: KadNode, packets: Union[Packet, Sequence[Packet]], *args: Any) -> T:
        not_list = False
        if not isinstance(packets, Sequence):
            not_list = True
            packets = [packets]
        for packet in packets:
            sender = cast(KadNode, packet.sender)
            self.update_bucket(sender)

        if not_list:
            return operation(self, *packets, *args)
        else:
            return operation(self, packets, *args)

    return wrapper


@dataclass
class KadNode(DHTNode):
    neigh: Optional[KadNode] = None
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=3)

    def __hash__(self):
        return self.id

    def find_value(self, packet: Packet, recv_req: Request) -> SimpyProcess:
        """Get value associated to a given key"""
        pass

    def store_value(self, packet: Packet, recv_req: Request) -> SimpyProcess:
        """Store the value to be associated to a given key"""
        pass

    def __post_init__(self):
        super().__post_init__()
        self.buckets = [[] for _ in range(self.log_world_size)]

    def get_bucket_for(self, key: int) -> int:
        dst = KadNode._compute_distance(key, self.id, self.log_world_size)
        return dst if dst == 0 else int(log2(dst))

    def update_bucket(self, node: KadNode) -> None:
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
                pass  # TODO: ping least recently seen node and swap with the new one if needed

    def find_node(self, key: int) -> SimpyProcess[List[KadNode]]:
        contacted = set()
        contacted.add(self)
        current = yield from self.find_neighbors(key, self.k)
        found = False
        while not found:
            requests = yield from self.ask_neighbors(current, contacted, key, self.k)
            # print(contacted)
            # print(current)
            packets: List[Packet] = []
            try:
                yield from self.wait_resps(requests, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error")

            # print(f"Received {packets}")
            found = yield from self.update_candidates(packets, key, current)

        for node in current:
            self.update_bucket(node)
        return current

    @packet_service
    @process_sender
    def update_candidates(self, packets: Sequence[Packet], key: int, current: List[KadNode]) -> bool:
        current_set = set(current)
        for packet in packets:
            for neigh in packet.data["neighbors"]:
                current_set.add(neigh)
        # print(current_set)

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
    def on_find_node_request(self, packet: Packet, recv_req: Request) -> None:
        neighs = yield from self.find_neighbors(packet.data["key"], packet.data["k"])
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
        # print(f"{self.name} Contacting {len(to_contact)} nodes")
        requests = []
        for node in to_contact:
            packet = Packet()
            packet.data["key"] = key
            packet.data["k"] = k
            sent_req = self.send_req(node.on_find_node_request, packet)
            requests.append(sent_req)
        return requests

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        # keys are log_world_size bits long
        return key1 ^ key2

    def join_network(self, to: DHTNode) -> SimpyProcess[None]:
        to = cast(KadNode, to)
        self.update_bucket(to)
        yield from self.find_node(self.id)


class NeighborPicker:
    def __init__(self, node: KadNode, key: int) -> None:
        index = node.get_bucket_for(key)
        self.current_nodes = node.buckets[index]
        self.left_buckets = node.buckets[:index]
        self.right_buckets = node.buckets[(index + 1):]
        self.left = True

    def __iter__(self) -> NeighborPicker:
        return self

    def __next__(self) -> KadNode:
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
