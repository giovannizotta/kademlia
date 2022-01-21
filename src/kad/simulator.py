from kad.node import Node
from kad.rbg import RandomBatchGenerator as RBG
from kad.utils import *


class Simulator:
    def __init__(
        self,
        env: simpy.Environment,
        nodes: Iterable[Node],
        mean_arrival: float = 0.50,
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
        keys = tuple(range(1000))
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            packet += 1
            print("%5.1f Arrival of packet %d" % (self.env.now, packet))
            # send it to a random node
            # node = self.get_random_node()
            node = self.nodes[0]
            # make it serve the request
            tmp_event = simpy.Event(self.env)
            key = self.rbg.get_choice(keys)
            self.env.process(
                node.wait_request(
                    packet,
                    tmp_event,
                    node.get_key_request,
                    dict(key=key,forward=True)
                )
            )
