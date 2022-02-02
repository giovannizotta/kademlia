from abc import ABC, abstractmethod
from common.node import DHTNode
from common.utils import *
import simpy, networkx as nx, matplotlib.pyplot as plt

class NetManager(ABC):

    def __init__(self, env: simpy.Environment, n_nodes:int, log_world_size:int) -> None:
        self.nodes : List[DHTNode] = list() 
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
    def prepare_updates(self) -> List[simpy.Event]:
        pass
