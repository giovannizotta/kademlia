from common.utils import *
from common.node import Node
from kad.node import KadNode
from common.simulator import Simulator
from common.rbg import RandomBatchGenerator as RBG

N_NODES = 10
MAX_TIME = 10.0


def create_nodes(env: simpy.Environment, n_nodes: int) -> Sequence[Node]:
    """Instantiate the nodes for the simulation"""
    nodes: List[KadNode] = list()
    for i in range(n_nodes):
        nodes.append(KadNode(env, f"node_{i}"))
    # hardwire ring
    nodes[0].neigh = nodes[1]
    nodes[1].neigh = nodes[2]
    nodes[2].neigh = nodes[0]
    return nodes


def main() -> None:
    # init random seed
    RBG(seed=42)
    env = simpy.Environment()
    nodes = create_nodes(env, N_NODES)
    simulator = Simulator(env, nodes)
    env.process(simulator.simulate())
    env.run(until=MAX_TIME)


if __name__ == "__main__":
    main()
