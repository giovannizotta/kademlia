from chord.node import ChordNode
from common.utils import *
from common.node import Node
from kad.node import KadNode
from common.simulator import Simulator
from common.rbg import RandomBatchGenerator as RBG
from argparse import ArgumentParser, Namespace
import logging

NODES_TO_JOIN = 10
MAX_TIME = 10.0
WORLD_SIZE = 160


def create_nodes(env: simpy.Environment, join: int) -> Sequence[Node]:
    """Instantiate the nodes for the simulation"""
    nodes: List[ChordNode] = list()
    for i in range(join):
        nodes.append(ChordNode(env, f"node_{i}", log_world_size=WORLD_SIZE))
    # hardwire two nodes
    nodes[0].succ = nodes[1]
    nodes[1].succ = nodes[0]
    nodes[0].pred = nodes[1]
    nodes[1].pred = nodes[0]
    return nodes


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    ap.add_argument("-t", "--max-time", type=float, default=10.0,
                    help="Maximum time to run the simulation")
    ap.add_argument("-n", "--nodes", type=int, default=NODES_TO_JOIN,
                    help="Number of nodes that will join the network at the beginning")
    ap.add_argument("-s", "--seed", type=int, default=42,
                    help="Random seed")
    ap.add_argument("-l", "--loglevel", type=str, default=logging.INFO,
                    help="Logging level")
    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument("-d", "--dht", choices=['kad', 'chord'], help="DHT to use")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    logger = logging.getLogger("logger")
    logger.setLevel(args.loglevel)
    fh = logging.FileHandler("logs.log", mode='w')
    fh.setLevel(args.loglevel)
    formatter = logging.Formatter('%(levelname)10s: %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # init random seed
    RBG(seed=args.seed)
    env = simpy.Environment()
    nodes = create_nodes(env, args.nodes)
    simulator = Simulator(env, nodes)
    env.process(simulator.simulate())
    env.run(until=args.max_time)

if __name__ == "__main__":
    main()
