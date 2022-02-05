from __future__ import annotations
from concurrent.futures import process
from common.utils import *
from common.node import DHTNode, packet_service, Packet, Request
from math import log2
from dataclasses import dataclass
from collections.abc import Sequence
from collections import defaultdict


def process_sender(operation: Callable[..., T]) -> Callable[..., T]:
    def wrapper(self: KadNode, packets: Union[Packet, Sequence[Packet]], *args: Any) -> T:
        is_list = True
        if not isinstance(packets, Sequence):
            is_list = False
            packets = [packets]
        for packet in packets:
            sender = cast(KadNode, packet.sender)
            self.log(f"processing sender {sender.name}")
            self.update_bucket(sender)
        if is_list:
            return operation(self, packets, *args)
        else:
            return operation(self, *packets, *args)

    return wrapper

def decide_value(packets: List[Packet]):
    # give the most popular value
    d = defaultdict(int)
    for packet in packets:
        if d[packet.data["value"]] is not None:
            d[packet.data["value"]] += 1
    if d:
        return max(d, key = lambda k: d[k])
    else: 
        return None

@dataclass
class KadNode(DHTNode):
    neigh: Optional[KadNode] = None
    buckets: List[List[KadNode]] = field(init=False, repr=False)
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=5)

    def __hash__(self):
        return self.id

    def __post_init__(self) -> None:
        super().__post_init__()
        self.max_timeout = 10

    @packet_service
    @process_sender
    def get_value(self, packet: Packet, recv_req: Request) -> None:
        super().get_value(packet, recv_req)

    @packet_service
    @process_sender
    def set_value(self, packet: Packet, recv_req: Request) -> None:
        super().set_value(packet, recv_req)

    @packet_service
    def reply_find_value(self, recv_req: Request, packets: List[Packet], hops: int) -> None:
        value = decide_value(packets)
        packet = Packet(data=dict(value=value, hops=hops))
        self.send_resp(recv_req, packet)

    @packet_service
    def ask_value(self, to: List[KadNode], packet: Packet) -> List[Request]:
        requests = []
        for node in to:
            self.log(f"asking {node} for the key {packet.data['key']}")
            sent_req = self.send_req(node.get_value, packet)
            requests.append(sent_req)
        return requests

    def find_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        """Find the value associated to a given key"""
        key = packet.data["key"]
        packets = []
        nodes, hops = yield from self.find_node(key)
        try:
            requests = yield from self.ask_value(nodes, packet)
            yield from self.wait_resps(requests, packets)
        except DHTTimeoutError:
            if not packets:
                hops = -1
                packet.data["value"] = None
        
        yield from self.reply_find_value(recv_req, packets, hops)

    @packet_service
    def ask_set_value(self, to: List[KadNode], packet: Packet) -> List[Request]:
        requests = []
        for node in to:
            self.log(
                    f"asking {node} to set the value {packet.data['value']} for the key {packet.data['key']}")
            sent_req = self.send_req(node.set_value, packet)
            requests.append(sent_req)
        return requests

    @packet_service
    def reply_store_value(self, recv_req: Request, hops:int) -> None:
        packet = Packet(data=dict(hops=hops))
        self.send_resp(recv_req, packet)

    def store_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        """Store the value to be associated to a given key"""
        key = packet.data["key"]
        packets = []
        try:
            nodes, hops = yield from self.find_node(key)
            requests = yield from self.ask_set_value(nodes, packet)
            yield from self.wait_resps(requests, packets)
        except DHTTimeoutError:
            hops = -1
            packet.data["value"] = None
        
        yield from self.reply_store_value(recv_req, hops)

    def __post_init__(self):
        super().__post_init__()
        self.buckets = [[] for _ in range(self.log_world_size)]

    def get_bucket_for(self, key: int) -> int:
        dst = KadNode._compute_distance(key, self.id, self.log_world_size)
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
        current = self.find_neighbors(key, self.k)
        found = False
        hop = 0
        while not found:
            requests = yield from self.ask_neighbors(current, contacted, key, self.k)
            hop += 1
            packets: List[Packet] = []
            try:
                yield from self.wait_resps(requests, packets)
            except DHTTimeoutError:
                self.log("DHT timeout error", level=logging.WARNING)


            # print(f"Received {packets}")
            found = yield from self.update_candidates(packets, key, current)

        for node in current:
            self.update_bucket(node)
        return current, hop

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

    def find_neighbors(self, key: int, k: int) -> List[KadNode]:
        nodes = set()
        for neigh in neigh_picker(self, key):
            nodes.add(neigh)
            if len(nodes) == k:
                break
        return sorted(nodes, key=lambda x: KadNode._compute_distance(x.id, key, self.log_world_size))

    @packet_service
    @process_sender
    def on_find_node_request(self, packet: Packet, recv_req: Request) -> None:
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
        # print(f"{self.name} Contacting {len(to_contact)} nodes")
        self.log(f"Contacting {to_contact} ")
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
