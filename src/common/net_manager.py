from abc import ABC, abstractmethod
from common.node import DHTNode, DataCollector
from common.utils import *
import simpy
import networkx as nx
import matplotlib.pyplot as plt
from dataclasses import dataclass, field

@dataclass
class NetManager(ABC):
    env: simpy.Environment
    n_nodes: int
    datacollector: DataCollector
    log_world_size: int
    nodes: Sequence[DHTNode] = field(init=False)

    NODE_SIZE: ClassVar[float] = 3

    NODE_COLOR: ClassVar[str] = "#89c2d9"
    SOURCE_COLOR: ClassVar[str] = "#f9844a"
    TARGETS_COLOR: ClassVar[str] = "#277da1"

    def __post_init__(self) -> None:
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
