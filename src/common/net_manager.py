from abc import ABC, abstractmethod
from common.node import DHTNode
from common.utils import *
import simpy
import networkx as nx
import matplotlib.pyplot as plt


class NetManager(ABC):

    NODE_SIZE: ClassVar[float] = 1200

    NODE_COLOR: ClassVar[str] = "#89c2d9"
    SOURCE_COLOR: ClassVar[str] = "#f9844a"
    TARGETS_COLOR: ClassVar[str] = "#277da1"

    def __init__(self, env: simpy.Environment, n_nodes: int, log_world_size: int) -> None:
        self.nodes: Sequence[DHTNode] = list()
        self.env = env
        self.n_nodes = n_nodes
        self.log_world_size = log_world_size
        self.create_nodes()

    @abstractmethod
    def create_nodes(self):
        pass

    @abstractmethod
    def print_network(self, node: DHTNode) -> None:
        pass

    @abstractmethod
    def prepare_updates(self) -> List[simpy.Process]:
        pass

    def change_env(self, env: simpy.Environment) -> None:
        self.env = env
        for node in self.nodes:
            node.change_env(env)
