from common.utils import *
from common.node import Node
from kad.node import KadNode
from common.simulator import Simulator
from common.rbg import RandomBatchGenerator as RBG
from argparse import ArgumentParser, Namespace

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


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    ap.add_argument("-t", "--max-time", type=float, default=10.0,
                    help="Maximum time to run the simulation")
    ap.add_argument("-n", "--nodes", type=int, default=10,
                    help="Number of nodes in the network")
    ap.add_argument("-s", "--seed", type=int, default=42,
                    help="Number of nodes in the network")
    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument("-d", "--dht", choices=['kad', 'chord'], help="DHT to use")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    # init random seed
    RBG(seed=args.seed)
    env = simpy.Environment()
    nodes = create_nodes(env, args.nodes,
                         # args.dht
                         )
    simulator = Simulator(env, nodes)
    env.process(simulator.simulate())
    env.run(until=args.max_time)


if __name__ == "__main__":
    main()
