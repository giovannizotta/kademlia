from common.net_manager import *
from chord.node import ChordNode
from common.utils import *


class ChordNetManager(NetManager):

    def create_nodes(self) -> None:
        self.nodes: Sequence[ChordNode] = list()
        for i in range(self.n_nodes):
            self.nodes.append(
                ChordNode(self.env, f"node_{i:05d}", self.datacollector, log_world_size=self.log_world_size, queue_capacity=self.capacity))
        # hardwire two nodes
        self.nodes[0].succ = self.nodes[1]
        self.nodes[1].succ = self.nodes[0]
        self.nodes[0].pred = self.nodes[1]
        self.nodes[1].pred = self.nodes[0]

    def print_network(self, node: DHTNode, ext: str) -> None:
        node = cast(ChordNode, node)
        graph_edges = [(u.id, u.succ.id)
                       for u in sorted(self.nodes, key=lambda n: n.id)]
        ft_edges = set([(node.id, finger.id) for finger in node.ft])
        G = nx.DiGraph()
        G.add_edges_from(graph_edges)
        # get nodes color
        color_map = {n: NetManager.NODE_COLOR for n in G.nodes}
        assert node.id in color_map
        for finger in node.ft:
            color_map[finger.id] = NetManager.TARGETS_COLOR
        color_map[node.id] = NetManager.SOURCE_COLOR
        colors = [color_map[n] for n in G.nodes]

        # draw ring
        plt.figure(figsize=(15, 15))
        pos = nx.circular_layout(G)
        nx.draw(G, pos, with_labels=False, node_size=NetManager.NODE_SIZE, node_color=colors,
                connectionstyle="arc3,rad=0.05")

        # draw ft edges
        nx.draw_networkx_edges(G, pos, edgelist=ft_edges, node_size=NetManager.NODE_SIZE,
                               edge_color="darkgrey", arrowstyle="->", connectionstyle="arc3,rad=-0.2")
        # plt.savefig("chord.png")
        plt.savefig(f"chord.{ext}", format=ext, bbox_inches=0, pad_inches=0)
        plt.show()

    def prepare_updates(self) -> SimpyProcess[None]:
        for node in self.nodes:
            yield self.env.process(node.update())
