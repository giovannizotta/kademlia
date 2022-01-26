from common.node import Node
from common.rbg import RandomBatchGenerator as RBG
from common.utils import *
from common.packet import *
import networkx as nx
import matplotlib.pyplot as plt
import logging


class Simulator:
    def __init__(
        self,
        env: simpy.Environment,
        nodes: Iterable[Node],
        mean_arrival: float = 1.0,
    ):
        self.env = env
        self.nodes = tuple(nodes)
        self.mean_arrival = mean_arrival
        self.rbg = RBG(seed=42)
        self.logger = logging.getLogger("logger")
        
    def log(self, msg: str, level: int = logging.DEBUG) -> None:
        self.logger.log(level, f" {self.env.now:5.1f} SIMULATOR: {msg}")

    def get_arrival_time(self) -> float:
        return self.rbg.get_exponential(self.mean_arrival)

    def get_random_node(self) -> Node:
        return self.rbg.get_choice(self.nodes)

    def build_network(self) -> SimpyProcess:
        for i in range(2, len(self.nodes)):
            yield self.env.process(
                self.nodes[i].join_network(self.nodes[0])
            )
        self.log("All nodes joined", level=logging.INFO)
        
    def print_network(self, node) -> None:
        with open("test.tmp", 'w') as f:
            graph_edges = [(u.id, u.succ.id) for u in sorted(self.nodes, key=lambda n: n.id)]
            f.write(f"before: {graph_edges}\n")
            graph_edges.extend([(node.id, finger.id) for finger in node.ft])
            f.write(f"after: {graph_edges}\n")
            f.write(f"ft size: {len(node.ft)}\n")
            f.write(f"{node.id} ft: {[n.id for n in node.ft]}")
            G = nx.DiGraph()
            G.add_edges_from(graph_edges)
            f.write(f"graph_edges: {len(G.edges)}\n")
            f.write(f"{len(set(G[node.id]))}\n")

            plt.figure(figsize=(14,14))
            nx.draw(G, pos=nx.circular_layout(G), with_labels=False, node_size=0.3)
            # plt.savefig("graph.png")
            plt.show()

    def simulate(self) -> SimpyProcess:
        yield from self.build_network()
        keys = tuple(range(1000))
        updates = []
        for node in self.nodes:
            updates.append(self.env.process(node.update()))

        yield simpy.AllOf(self.env, updates)
        self.log(f"Updates are done for all nodes.", level=logging.INFO)
        
        # self.print_network(self.nodes[0])
        
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            key = self.rbg.get_choice(keys)
            packet = Packet(data=dict(key=key))
            self.log("%5.1f Arrival of packet %d" % (self.env.now, packet.id))
            # send it to a random node
            # node = self.get_random_node()
            node = self.nodes[0]
            # make the node send a request to itself
            _ = node.send_req(node.find_node_request, packet)

        
