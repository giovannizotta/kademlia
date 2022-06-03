from dataclasses import dataclass, field
from typing import List, Sequence

import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_pydot import pydot_layout

from common.net_manager import NetManager
from common.utils import SimpyProcess
from kad.node import KadNode


def get_key(id: int) -> str:
    return f"{id:0160b}"


class Trie(nx.DiGraph):
    def __init__(self):
        super().__init__()
        self.root = "A"
        self.add_node(self.root, bit="A")

    def get_labels(self):
        return {(a, b): self.nodes[b]["bit"] for a, b in self.edges()}

    def add(self, to_add: str) -> None:
        node = self.root
        prefix = ""
        for bit in to_add:
            prefix += bit
            for child in self[node]:
                if self.nodes[child]["bit"] == bit:
                    node = child
                    break
            else:
                self.add_node(prefix, bit=bit)
                self.add_edge(node, prefix)
                node = prefix

    def prune(self, node):
        prunable = len(self[node]) <= 1
        for child in self[node]:
            prunable = self.prune(child) and prunable

        if prunable and len(self[node]) > 0:
            child = next(iter(self[node]))
            self.remove_node(child)
        return prunable

    def find_prefix(self, to_add: str) -> str:
        node = self.root
        prefix = ""
        for bit in to_add:
            for child in self[node]:
                if self.nodes[child]["bit"] == bit:
                    prefix += bit
                    node = child
                    break
            else:
                return prefix
        return prefix

    def get_sorted(self):
        """Return a copy where adjacency list is sorted"""
        new_trie = Trie()
        stack = [self.root]
        while stack:
            node = stack.pop()
            for child in sorted(self[node]):
                new_trie.add(child)
                stack.append(child)
        return new_trie


@dataclass
class KadNetManager(NetManager):
    alpha: int = field(repr=False, default=3)
    k: int = field(repr=False, default=5)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.trie = Trie()
        for node in self.nodes:
            self.trie.add(get_key(node.id))

        self.trie.prune(self.trie.root)
        self.trie = self.trie.get_sorted()

    def get_new_node(self) -> KadNode:
        return KadNode(
            self.env,
            self.datacollector,
            log_world_size=self.log_world_size,
            queue_capacity=self.capacity,
            alpha=self.alpha,
            k=self.k,
        )

    def create_nodes(self) -> None:
        self.nodes: Sequence[KadNode] = list()
        for _ in range(self.n_nodes):
            self.nodes.append(
                KadNode(
                    self.env,
                    self.datacollector,
                    log_world_size=self.log_world_size,
                    queue_capacity=self.capacity,
                    alpha=self.alpha,
                    k=self.k,
                )
            )
        # hardwire two nodes
        self.nodes[0].update_bucket(self.nodes[1])
        self.nodes[1].update_bucket(self.nodes[0])

    def print_network(self, node: KadNode, ext: str) -> None:

        # add buckets edges
        buckets_edges = []

        color_map = {node: NetManager.NODE_COLOR for node in self.trie.nodes}
        a_pfx = self.trie.find_prefix(get_key(node.id))
        color_map[a_pfx] = NetManager.SOURCE_COLOR
        for bucket in node.buckets:
            if len(bucket) > 0:
                for b in bucket[:1]:
                    # print(get_key(b.id))
                    b_pfx = self.trie.find_prefix(get_key(b.id))
                    color_map[b_pfx] = NetManager.TARGETS_COLOR
                    buckets_edges.append((a_pfx, b_pfx))
        colors = [color_map[n] for n in self.trie.nodes]

        # get nodes size
        ns: List[int] = []
        for n in self.trie.nodes():
            if self.trie.out_degree(n) == 0:
                ns.append(NetManager.NODE_SIZE)
            else:
                ns.append(0)

        # draw trie
        plt.figure(figsize=(20, 12))
        pos = pydot_layout(self.trie, prog="dot")
        nx.draw(
            self.trie,
            pos,
            with_labels=False,
            node_size=ns,
            node_color=colors,
            arrowstyle="-",
        )
        nx.draw_networkx_edge_labels(
            self.trie, pos, edge_labels=self.trie.get_labels(), rotate=False
        )

        # draw buckets edges
        nx.draw_networkx_edges(
            self.trie,
            pos,
            edgelist=buckets_edges,
            node_size=ns,
            edge_color="darkgrey",
            arrowstyle="-|>",
            connectionstyle="arc3,rad=-0.2",
        )

        plt.savefig(f"img/kad.{ext}", format=ext, bbox_inches=0, pad_inches=0)
        plt.show()

    def prepare_updates(self) -> SimpyProcess[None]:
        yield from []
