from common.net_manager import *
from chord.node import ChordNode
from common.utils import *


class ChordNetManager(NetManager):

    def create_nodes(self) -> None:
        """Instantiate the nodes for the simulation"""
        self.nodes: Sequence[ChordNode] = list()
        for i in range(self.n_nodes):
            self.nodes.append(
                ChordNode(self.env, f"node_{i:05d}", log_world_size=self.log_world_size))
        # hardwire two nodes
        self.nodes[0].succ = self.nodes[1]
        self.nodes[1].succ = self.nodes[0]
        self.nodes[0].pred = self.nodes[1]
        self.nodes[1].pred = self.nodes[0]

    def print_network(self, node: DHTNode) -> None:
        node = cast(ChordNode, node)
        graph_edges = [(u.id, u.succ.id)
                       for u in sorted(self.nodes, key=lambda n: n.id)]
        graph_edges.extend([(node.id, finger.id) for finger in node.ft])
        G = nx.DiGraph()
        G.add_edges_from(graph_edges)

        plt.figure(figsize=(14, 14))
        nx.draw(G, pos=nx.circular_layout(G),
                with_labels=False, node_size=0.3)
        # plt.savefig("chord.png")
        plt.show()

    def prepare_updates(self) -> List[simpy.Process]:
        updates = []
        for node in self.nodes:
            updates.append(self.env.process(node.update()))
        return updates
