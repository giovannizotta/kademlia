from __future__ import annotations

import json
import logging
from argparse import ArgumentParser, Namespace

from simpy.core import Environment

from chord.net_manager import ChordNetManager
from common.collector import DataCollector
from common.net_manager import NetManager
from common.utils import RandomBatchGenerator as RBG
from kad.net_manager import KadNetManager
from simulation.constants import DEFAULT_LOG_WORLD_SIZE, DEFAULT_MAX_TIME, DEFAULT_NODES, DEFAULT_SEED, DEFAULT_LOGGING, \
    DEFAULT_PLOT, DEFAULT_CLIENT_RATE, DEFAULT_EXT, DEFAULT_ALPHA, DEFAULT_K, DEFAULT_QUEUE_CAPACITY, DEFAULT_N_KEYS, \
    DEFAULT_JOINLAMBDA1, DEFAULT_JOINLAMBDA2, DEFAULT_JOINRATE, DEFAULT_CRASHMEAN, DEFAULT_CRASHVARIANCE, \
    DEFAULT_CRASHRATE, DEFAULT_JOIN_P, DEFAULT_MAX_VALUE, DEFAULT_MEAN_SERVICE_TIME, DEFAULT_PEER_TIMEOUT, \
    DEFAULT_STABILIZE_PERIOD_MEAN, DEFAULT_STABILIZE_PERIOD_STDDEV, DEFAULT_STABILIZE_PERIOD_MIN, \
    DEFAULT_UPDATE_PERIOD_MEAN, DEFAULT_UPDATE_PERIOD_STDDEV, DEFAULT_UPDATE_PERIOD_MIN, DEFAULT_CLIENT_TIMEOUT
from simulation.simulator import Simulator


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    ap.add_argument(
        "-t",
        "--max-time",
        type=int,
        default=DEFAULT_MAX_TIME,
        help="Maximum time to run the simulation",
    )
    ap.add_argument(
        "-n",
        "--nodes",
        type=int,
        default=DEFAULT_NODES,
        help="Number of nodes that will join the network at the beginning",
    )
    ap.add_argument("-s", "--seed", type=int, default=DEFAULT_SEED, help="Random seed")
    ap.add_argument(
        "-l", "--loglevel", type=str, default=DEFAULT_LOGGING, help="Logging level"
    )
    ap.add_argument(
        "-p", "--plot", type=bool, default=DEFAULT_PLOT, help="Plot the network graph"
    )
    ap.add_argument(
        "-r",
        "--rate",
        type=float,
        default=DEFAULT_CLIENT_RATE,
        help="Client arrival rate (lower is faster)",
    )
    ap.add_argument(
        "-e",
        "--ext",
        default=DEFAULT_EXT,
        choices=["pdf", "png"],
        help="Extension for network plots",
    )
    ap.add_argument(
        "-a", "--alpha", type=int, default=DEFAULT_ALPHA, help="Alpha value for Kademlia"
    )
    ap.add_argument("-k", "--k", type=int, default=DEFAULT_K, help="K value for Kademlia")
    ap.add_argument(
        "-q", "--capacity", type=int, default=DEFAULT_QUEUE_CAPACITY, help="Queue capacity"
    )
    ap.add_argument(
        "--nkeys", type=int, default=DEFAULT_N_KEYS, help="Number of keys"
    )
    ap.add_argument(
        "-d",
        "--dht",
        required=True,
        choices=[Simulator.KAD, Simulator.CHORD],
        help="DHT to use",
    )
    ap.add_argument(
        "--joinrate", type=float, default=DEFAULT_JOINRATE, help="Parameter for tuning the join distribution"
    )
    ap.add_argument(
        "--crashrate", type=float, default=DEFAULT_CRASHRATE, help="Parameter for tuning the crash distribution"
    )

    return ap.parse_args()


def get_filename(args) -> str:
    return "data.json"
    # return os.path.join(args.datadir, f"{args.dht}_{args.nodes}_nodes_{args.max_time}_time_{args.rate:.01f}_rate.json")


def main() -> None:
    args = parse_args()
    # init the logger
    logger = logging.getLogger("logger")
    logger.setLevel(args.loglevel)
    fh = logging.FileHandler(f"{args.dht}_logs.log", mode="w")
    fh.setLevel(args.loglevel)
    formatter = logging.Formatter("%(levelname)10s: %(message)s")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    # init random seed
    RBG(seed=args.seed)

    # init the network
    join_env = Environment()
    datacollector = DataCollector()
    keys = list(map(lambda x: f"key_{x}", range(args.nkeys)))
    net_manager: NetManager
    if args.dht == Simulator.KAD:
        net_manager = KadNetManager(
            join_env,
            args.nodes,
            datacollector,
            DEFAULT_LOG_WORLD_SIZE,
            args.capacity,
            DEFAULT_CRASHMEAN,
            DEFAULT_CRASHVARIANCE,
            args.crashrate,
            DEFAULT_MEAN_SERVICE_TIME,
            DEFAULT_PEER_TIMEOUT,
            args.alpha,
            args.k,
        )
    else:
        assert args.dht == Simulator.CHORD
        net_manager = ChordNetManager(
            join_env,
            args.nodes,
            datacollector,
            DEFAULT_LOG_WORLD_SIZE,
            args.capacity,
            DEFAULT_CRASHMEAN,
            DEFAULT_CRASHVARIANCE,
            args.crashrate,
            DEFAULT_MEAN_SERVICE_TIME,
            DEFAULT_PEER_TIMEOUT,
            args.k,
            DEFAULT_STABILIZE_PERIOD_MEAN,
            DEFAULT_STABILIZE_PERIOD_STDDEV,
            DEFAULT_STABILIZE_PERIOD_MIN,
            DEFAULT_UPDATE_PERIOD_MEAN,
            DEFAULT_UPDATE_PERIOD_STDDEV,
            DEFAULT_UPDATE_PERIOD_MIN,
        )

    simulator = Simulator(
        join_env,
        net_manager,
        keys,
        DEFAULT_MAX_VALUE,
        args.plot,
        args.ext,
        args.rate,
        DEFAULT_CLIENT_TIMEOUT,
        DEFAULT_MEAN_SERVICE_TIME,
        DEFAULT_JOINLAMBDA1,
        DEFAULT_JOINLAMBDA2,
        args.joinrate,
        DEFAULT_JOIN_P,
    )
    join_env.process(simulator.simulate_join())
    join_env.run()

    # forget data collected during the join
    datacollector.clear()
    # start the simulation
    run_env = Environment()
    simulator.change_env(run_env)
    run_env.process(simulator.simulate_clients())
    run_env.process(simulator.simulate_joins())
    run_env.run(until=args.max_time)

    print(
        f"Total nodes: {len(net_manager.nodes)}, "
        f"Healthy nodes: {len(net_manager.healthy_nodes)}, "
        f"Failed to join: {len(datacollector.failed_to_join)}"
    )

    # dump collected data
    with open(get_filename(args), "w", encoding="utf8") as f:
        json.dump(datacollector.to_dict(), f, separators=(",", ":"))


if __name__ == "__main__":
    main()
