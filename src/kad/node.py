from __future__ import annotations
from common.utils import *
from common.node import DHTNode, packet_service
from common.packet import Packet
from math import log2
from dataclasses import dataclass

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
        last = yield from self.find_neighbors(key, self.k)
        while True:
            candidates = last.copy()
            to_contact = []
            for node in last:
                if node not in contacted:
                    contacted.add(node)
                    to_contact.append(node)   
                if len(to_contact) == self.alpha:
                    break
            requests = yield from self.ask_neighbors(to_contact, key, self.k)

            packets: List[Packet] = []
            try:
                yield from self.wait_resps(requests, packets) 
            except DHTTimeoutError:
                self.log("DHT timeout error")
           
            current_set = set(candidates)
            for packet in packets:
                for neigh in packet.data["neighbors"]:
                    if neigh not in current_set:
                        candidates.append(neigh)
                        current_set.add(neigh)

            candidates.sort(key = lambda x: KadNode._compute_distance(x.id, key, self.log_world_size))
            candidates = candidates[:self.k]
            if candidates != last:
                last = candidates
            else:
                break

        return last

    @packet_service
    def find_neighbors(self, key:int, k: int) -> List[KadNode]:
        nodes = set() 
        for neigh in NeighborPicker(self, key):
            nodes.add(neigh)
            if len(nodes) == k:
                break
        return sorted(nodes, key = lambda x: KadNode._compute_distance(x.id, key, self.log_world_size))

    @packet_service
    def get_neighbors(self, packet: Packet, recv_req: simpy.Event) -> None:
        neighs = self.find_neighbors(packet.data["key"], packet.data["k"])
        packet.data["neighbors"] = neighs
        self.send_resp(recv_req, packet)

    @packet_service
    def ask_neighbors(self, to_contact:List[KadNode], key:int, k:int) -> List[simpy.Event]:
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
        index = int(log2(key))
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

