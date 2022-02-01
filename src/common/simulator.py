from chord.node import ChordNode
from common.node import DHTNode
from common.utils import *
from common.client import Client
import networkx as nx
import matplotlib.pyplot as plt
import logging


@dataclass
class Simulator(Loggable):
    nodes: Sequence[DHTNode]
    keys: Sequence[str]
    max_value: int = 10**9
    mean_arrival: float = 1.0

    FIND: ClassVar[str] = "FIND"
    STORE: ClassVar[str] = "STORE"
    CLIENT_ACTIONS: ClassVar[Tuple[str, str]] = (FIND, STORE)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.nodes: Tuple[DHTNode] = tuple(self.nodes)
        self.keys: Tuple[DHTNode] = tuple(self.keys)
        self.id = -1

    def get_arrival_time(self) -> float:
        return self.rbg.get_exponential(self.mean_arrival)

    def get_random_node(self) -> DHTNode:
        n_id = self.rbg.get_from_range(len(self.nodes))
        return self.nodes[n_id]

    def get_random_action(self) -> str:
        a_id = self.rbg.get_from_range(2)
        return Simulator.CLIENT_ACTIONS[a_id]

    def get_random_key(self) -> str:
        k_id = self.rbg.get_from_range(len(self.keys))
        return self.keys[k_id]

    def build_network(self) -> SimpyProcess:
        for i in range(2, len(self.nodes)):
            yield self.env.process(
                self.nodes[i].join_network(self.nodes[0])
            )
        self.log("All nodes joined", level=logging.INFO)

    def print_network(self, node: ChordNode) -> None:
        with open("test.tmp", 'w') as f:
            graph_edges = [(u.id, u.succ.id)
                           for u in sorted(self.nodes, key=lambda n: n.id)]
            f.write(f"before: {graph_edges}\n")
            graph_edges.extend([(node.id, finger.id) for finger in node.ft])
            f.write(f"after: {graph_edges}\n")
            f.write(f"ft size: {len(node.ft)}\n")
            f.write(f"{node.id} ft: {[n.id for n in node.ft]}")
            G = nx.DiGraph()
            G.add_edges_from(graph_edges)
            f.write(f"graph_edges: {len(G.edges)}\n")
            f.write(f"{len(set(G[node.id]))}\n")

            plt.figure(figsize=(14, 14))
            nx.draw(G, pos=nx.circular_layout(G),
                    with_labels=False, node_size=0.3)
            # plt.savefig("graph.png")
            plt.show()

    def get_client_behaviour(self, client: Client) -> SimpyProcess:
        """Get random client action (find or store a key/value pair)"""
        action = self.get_random_action()
        key = self.get_random_key()
        # send it to a random node
        # node = self.get_random_node()
        ask_to = self.get_random_node()
        if action == Simulator.STORE:
            return client.find_value(ask_to, key)
        else:
            value = self.rbg.get_from_range(self.max_value)
            return client.store_value(ask_to, key, value)

    def simulate(self) -> SimpyProcess:
        yield from self.build_network()
        keys = tuple(range(1000))
        updates = []
        for node in self.nodes:
            updates.append(self.env.process(node.update()))

        yield simpy.AllOf(self.env, updates)
        self.log(f"Updates are done for all nodes.", level=logging.INFO)

        self.print_network(self.nodes[0])
        i = 0
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            client_name = f"client_{i:05d}"
            client = Client(self.env, client_name)
            proc = self.get_client_behaviour(client)
            self.env.process(proc)
            i += 1
