import math
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, List

from folium import Map
from folium.plugins import HeatMap

from common.collector import DataCollector
from common.node import DHTNode
from common.utils import Environment, LocationManager, Loggable, SimpyProcess


@dataclass
class NetManager(Loggable):
    """Manages a DHT network by creating and configuring the nodes"""

    n_nodes: int
    datacollector: DataCollector
    log_world_size: int
    capacity: int
    crash_mean: float
    crash_variance: float
    crash_rate: float
    nodes: List[DHTNode] = field(init=False)
    healthy_nodes: List[DHTNode] = field(init=False)
    location_manager: LocationManager = field(init=False, repr=False)
    crash_mu: float = field(init=False, repr=False)
    crash_sigma: float = field(init=False, repr=False)

    NODE_SIZE: ClassVar[int] = 1200

    NODE_COLOR: ClassVar[str] = "#89c2d9"
    SOURCE_COLOR: ClassVar[str] = "#f9844a"
    TARGETS_COLOR: ClassVar[str] = "#277da1"

    def __post_init__(self) -> None:
        super().__post_init__()
        self.location_manager = LocationManager()
        self.create_nodes()
        self.healthy_nodes: List[DHTNode] = list()
        for node in self.nodes:
            self.healthy_nodes.append(node)

        if self.crash_rate > 0:
            self.crash_mean /= self.crash_rate
            self.crash_mu = math.log(self.crash_mean / math.sqrt(self.crash_variance / (self.crash_mean ** 2) + 1))
            self.crash_sigma = math.sqrt(math.log(self.crash_variance / (self.crash_mean ** 2) + 1))

    @abstractmethod
    def _hardwire_nodes(self, node0: DHTNode, node1: DHTNode) -> None:
        """Create the DHT nodes"""
        pass

    @abstractmethod
    def get_new_node(self) -> DHTNode:
        pass

    def create_nodes(self) -> None:
        self.nodes = list()
        for _ in range(self.n_nodes):
            self.nodes.append(self.get_new_node())
        self._hardwire_nodes(self.nodes[0], self.nodes[1])

    @abstractmethod
    def print_network(self, node: DHTNode, ext: str) -> None:
        """Plot a graph of the network"""
        pass

    @abstractmethod
    def prepare_updates(self) -> SimpyProcess[None]:
        """Simulate an update of the network knowledge (if needed)"""
        pass

    def plot_heatmap(self) -> None:
        print("Plotting heatmap...", end=" ")
        m = Map()
        hm = HeatMap([x.location for x in self.nodes], radius=20)
        m.add_child(hm)
        m.save("heatmap.html")

    def change_env(self, env: Environment) -> None:
        self.env = env
        for node in self.nodes:
            node.change_env(env)
            self.env.process(self.schedule_node_crash(node))
            self.datacollector.joined_time[node.name] = 0.0
            node.collect_load()

    def get_healthy_node(self) -> DHTNode:
        return self.rbg.choose(self.healthy_nodes)

    def get_crash_time(self) -> float:
        if self.crash_rate == 0:
            return math.inf

        return 10 * 1000 * self.rbg.get_lognormal(self.crash_mu, self.crash_sigma)

    def schedule_node_crash(self, node: DHTNode) -> SimpyProcess[None]:
        t = self.get_crash_time()
        yield self.env.timeout(t)
        if len(self.healthy_nodes) > 2:
            node.crash()
            self.healthy_nodes.remove(node)

    def crash_next(self) -> None:
        node = self.get_healthy_node()
        self.env.process(self.schedule_node_crash(node))

    def make_node_join(self, node: DHTNode, ask_to: DHTNode) -> SimpyProcess[None]:
        joined = yield from node.join_network(ask_to)
        if joined:
            self.nodes.append(node)
            self.datacollector.joined_time[node.name] = self.env.now
            node.collect_load()
            self.healthy_nodes.append(node)
            self.env.process(self.schedule_node_crash(node))
        else:
            self.datacollector.failed_to_join.append(self.env.now)

    def join_next(self) -> None:
        node = self.get_new_node()
        ask_to = self.get_healthy_node()
        self.log(f"{node} trying to join, asking to {ask_to}")
        self.env.process(self.make_node_join(node, ask_to))
