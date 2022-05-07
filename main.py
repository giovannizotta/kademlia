import json
from argparse import ArgumentParser, Namespace

from tqdm import tqdm

from chord.net_manager import ChordNetManager
from common.net_manager import NetManager
from common.node import DataCollector
from common.simulator import Simulator
from common.utils import *
from common.utils import RandomBatchGenerator as RBG
from kad.net_manager import KadNetManager

NODES_TO_JOIN = 10
MAX_TIME = 10.0
WORLD_SIZE = 160
N_KEYS = 10 ** 4
QUEUE_CAPACITY = 100


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    ap.add_argument("-t", "--max-time", type=int, default=1000,
                    help="Maximum time to run the simulation")
    ap.add_argument("-n", "--nodes", type=int, default=NODES_TO_JOIN,
                    help="Number of nodes that will join the network at the beginning")
    ap.add_argument("-s", "--seed", type=int, default=42,
                    help="Random seed")
    ap.add_argument("-l", "--loglevel", type=str, default=logging.INFO,
                    help="Logging level")
    ap.add_argument("-p", "--plot", type=bool, default=False,
                    help="Plot the network graph")
    ap.add_argument("-r", "--rate", type=float, default=0.1,
                    help="Client arrival rate (lower is faster)")
    ap.add_argument("-f", "--file", type=str, required=True,
                    help="File in which to save data")
    ap.add_argument("-e", "--ext", default="pdf", choices=["pdf", "png"],
                    help="Extension for network plots")
    ap.add_argument("-a", "--alpha", type=int, default=3,
                    help="Alpha value for Kademlia")
    ap.add_argument("-k", type=int, default=5,
                    help="K value for Kademlia")
    ap.add_argument("-q", "--capacity", type=int, default=QUEUE_CAPACITY,
                    help="Queue capacity")

    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument("-d", "--dht", required=True,
                    choices=[Simulator.KAD, Simulator.CHORD], help="DHT to use")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    # init the logger
    logger = logging.getLogger("logger")
    logger.setLevel(args.loglevel)
    fh = logging.FileHandler(f"{args.dht}_logs.log", mode='w')
    fh.setLevel(args.loglevel)
    formatter = logging.Formatter('%(levelname)10s: %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # init random seed
    RBG(seed=args.seed)

    # init the network
    join_env = simpy.Environment()
    datacollector = DataCollector()
    keys = list(map(lambda x: f"key_{x}", range(N_KEYS)))
    net_manager: NetManager
    if args.dht == Simulator.KAD:
        net_manager = KadNetManager(
            join_env, args.nodes, datacollector, WORLD_SIZE, args.capacity, args.alpha, args.k)
    elif args.dht == Simulator.CHORD:
        net_manager = ChordNetManager(
            join_env, args.nodes, datacollector, WORLD_SIZE, args.capacity, args.k)

    simulator = Simulator(join_env, "Simulator", net_manager,
                          keys, args.plot, mean_arrival=args.rate, ext=args.ext)
    join_env.process(simulator.simulate_join())
    join_env.run()

    # forget data collected during the join
    datacollector.clear()
    # start the simulation
    run_env = simpy.Environment()
    run_env.process(simulator.simulate(run_env))
    for i in tqdm(range(args.max_time)):
        run_env.run(until=i + 1)

    # dump collected data
    with open(args.file, 'w', encoding='utf8') as f:
        json.dump(datacollector.to_dict(), f, separators=(',', ':'))


if __name__ == "__main__":
    main()
