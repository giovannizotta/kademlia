from kad.utils import *
from kad.node import Node
from kad.simulator import Simulator
from kad.rbg import RandomBatchGenerator as RBG

N_NODES = 10
MAX_TIME = 100.0


def create_nodes(env: simpy.Environment, n_nodes: int) -> List[Node]:
    """Instantiate the nodes for the simulation"""
    nodes = list()
    for i in range(N_NODES):
        nodes.append(Node(env, i))
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
