from common.node import Node
from common.rbg import RandomBatchGenerator as RBG
from common.utils import *
from common.packet import *


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

    def build_network(self) -> SimpyProcess:
        for i in range(2, len(self.nodes)):
            yield self.env.process(
                self.nodes[i].join_network(self.nodes[0])
            )

    def simulate(self) -> SimpyProcess:
        yield from self.build_network()
        keys = tuple(range(1000))
        updates = []
        for node in self.nodes:
            updates.append(self.env.process(node.update()))

        yield simpy.AllOf(self.env, updates)
        print(f"Updates are done for all nodes.")
        
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            packet = Packet()
            print("%5.1f Arrival of packet %d" % (self.env.now, packet.id))
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
                    node.find_node,
                    dict(key=key)
                )
            )

