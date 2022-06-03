from abc import abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, List

from common.collector import DataCollector
from common.node import DHTNode
from common.utils import Environment, Loggable, SimpyProcess


@dataclass
class NetManager(Loggable):
    """Manages a DHT network by creating and configuring the nodes"""

    n_nodes: int
    datacollector: DataCollector
    log_world_size: int
    capacity: int
    nodes: List[DHTNode] = field(init=False)
    healthy_nodes: List[DHTNode] = field(init=False)

    NODE_SIZE: ClassVar[int] = 1200

    NODE_COLOR: ClassVar[str] = "#89c2d9"
    SOURCE_COLOR: ClassVar[str] = "#f9844a"
    TARGETS_COLOR: ClassVar[str] = "#277da1"

    def __post_init__(self) -> None:
        super().__post_init__()
        self.create_nodes()
        self.healthy_nodes: List[DHTNode] = list()
        for node in self.nodes:
            self.healthy_nodes.append(node)

    @abstractmethod
    def create_nodes(self):
        """Create the DHT nodes"""
        pass

    @abstractmethod
    def print_network(self, node: DHTNode, ext: str) -> None:
        """Plot a graph of the network"""
        pass

    @abstractmethod
    def prepare_updates(self) -> SimpyProcess[None]:
        """Simulate an update of the network knowledge (if needed)"""
        pass

    def change_env(self, env: Environment) -> None:
        self.env = env
        for node in self.nodes:
            node.change_env(env)

    def get_healthy_node(self) -> DHTNode:
        return self.rbg.choose(self.healthy_nodes)

    def make_node_crash(self, node: DHTNode) -> SimpyProcess[None]:
        yield from node.crash()
        self.healthy_nodes.remove(node)

    def crash_next(self) -> None:
        node = self.get_healthy_node()
        self.env.process(self.make_node_crash(node))

    @abstractmethod
    def get_new_node(self) -> DHTNode:
        pass

    def make_node_join(self, node: DHTNode, ask_to: DHTNode) -> SimpyProcess[None]:
        yield from node.join_network(ask_to)
        self.nodes.append(node)
        self.healthy_nodes.append(node)

    def join_next(self) -> None:
        node = self.get_new_node()
        ask_to = self.get_healthy_node()
        self.log(f"{node} trying to join, asking to {ask_to}")
        self.env.process(self.make_node_join(node, ask_to))
