from kad.node import Node
from kad.rbg import RandomBatchGenerator as RBG
from kad.utils import *


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

    def get_arrival_time(self) -> float:
        return self.rbg.get_exponential(self.mean_arrival)

    def get_random_node(self) -> Node:
        return self.rbg.get_choice(self.nodes)

    def simulate(self) -> SimpyProcess:
        packet = 0
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            packet += 1
            print("%.1f Arrival of packet %d" % (self.env.now, packet))
            # send it to a random node
            node = self.get_random_node()
            # make it serve the request
            self.env.process(node.serve_request(packet))
