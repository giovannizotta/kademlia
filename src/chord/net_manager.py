from dataclasses import dataclass, field
from typing import List

import matplotlib.pyplot as plt
import networkx as nx

from chord.node import ChordNode
from common.net_manager import NetManager
from common.utils import SimpyProcess


@dataclass
class ChordNetManager(NetManager):
    k: int = field(repr=False, default=5)

    def create_nodes(self) -> None:
        self.nodes: List[ChordNode] = list()
        for i in range(self.n_nodes):
            self.nodes.append(
                ChordNode(
                    self.env,
                    self.datacollector,
                    log_world_size=self.log_world_size,
                    queue_capacity=self.capacity,
                    k=self.k,
                )
            )
        # hardwire two nodes
        for i in range(self.k):
            self.nodes[0].succ = (i, self.nodes[1])
            self.nodes[1].succ = (i, self.nodes[0])
            self.nodes[0].pred = (i, self.nodes[1])
            self.nodes[1].pred = (i, self.nodes[0])

    def get_new_node(self) -> ChordNode:
        return ChordNode(
            self.env,
            self.datacollector,
            log_world_size=self.log_world_size,
            queue_capacity=self.capacity,
            k=self.k,
        )

    def print_network(self, node: ChordNode, ext: str) -> None:
        graph_edges = []
        start = self.nodes[0]
        ptr = start.succ[0]
        while ptr != start:
            assert ptr is not None
            assert ptr.succ[0] is not None
            graph_edges.append((ptr.id, ptr.succ[0].id))
            ptr = ptr.succ[0]
        ft_edges = set([(node.id, finger.id) for finger in node.ft[0]])
        G = nx.DiGraph()
        G.add_edges_from(graph_edges)
        # get nodes color
        color_map = {n: NetManager.NODE_COLOR for n in G.nodes}
        assert node.id in color_map
        for finger in node.ft[0]:
            color_map[finger.id] = NetManager.TARGETS_COLOR
        color_map[node.id] = NetManager.SOURCE_COLOR
        colors = [color_map[n] for n in G.nodes]

        # draw ring
        plt.figure(figsize=(15, 15))
        pos = nx.circular_layout(G)
        nx.draw(
            G,
            pos,
            with_labels=False,
            node_size=NetManager.NODE_SIZE,
            node_color=colors,
            connectionstyle="arc3,rad=0.05",
        )

        # draw ft edges
        nx.draw_networkx_edges(
            G,
            pos,
            edgelist=ft_edges,
            node_size=NetManager.NODE_SIZE,
            edge_color="darkgrey",
            arrowstyle="->",
            connectionstyle="arc3,rad=-0.2",
        )
        # plt.savefig("chord.png")
        plt.savefig(f"img/chord.{ext}", format=ext, bbox_inches=0, pad_inches=0)
        plt.show()

    def prepare_updates(self) -> SimpyProcess[None]:
        for node in self.nodes:
            yield self.env.process(node.update())
