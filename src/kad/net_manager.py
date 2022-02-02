from common.net_manager import *
from kad.node import KadNode
from common.utils import *
from networkx.drawing.nx_pydot import graphviz_layout


class Trie(nx.DiGraph):
    
    def __init__(self):
        super().__init__()
        self.root = "A"
        self.add_node(self.root, bit="A")
        
    def get_labels(self):
        return {n: attr["bit"] for n, attr in self.nodes.items()}

    def add(self, to_add: KadNode):
        node = self.root
        prefix = ""
        for bit in bin(to_add.id)[2:]:
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
        
    def find_prefix(self, to_add: KadNode):
        node = self.root
        prefix = ""
        for bit in bin(to_add.id)[2:]:
            for child in self[node]:
                if self.nodes[child]["bit"] == bit:
                    prefix += bit
                    node = child
                    break
            else:
                return prefix


class KadNetManager(NetManager):

    def __init__(self, env: simpy.Environment, n_nodes:int, log_world_size:int) -> None:
        super().__init__(env, n_nodes, log_world_size)
        self.trie = Trie()
        for node in self.nodes:
            self.trie.add(node)

        self.trie.prune(self.trie.root)

    def create_nodes(self) -> None:
        """Instantiate the nodes for the simulation"""
        self.nodes: List[KadNode] = list()
        for i in range(self.n_nodes):
            self.nodes.append(KadNode(self.env, f"node_{i:05d}", log_world_size=self.log_world_size))
        # hardwire two nodes
        self.nodes[0].update_bucket(self.nodes[1])
        self.nodes[1].update_bucket(self.nodes[0])
        

    def print_network(self, node: KadNode) -> None:
        edges = []
        for bucket in node.buckets:
            if len(bucket) > 0:
                a = self.trie.find_prefix(node)
                for b in bucket:
                    b = self.trie.find_prefix(b)
                    print(a, b)
                    edges.append((a, b))
        
        self.trie.add_edges_from(edges)
        pos = graphviz_layout(self.trie, prog="dot")
        nx.draw(self.trie, pos, with_labels=True, labels=self.trie.get_labels(), node_size=1200)
        plt.show()

    def prepare_updates(self) -> List[simpy.Event]:
        return []

