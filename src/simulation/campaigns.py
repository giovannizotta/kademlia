from runexpy.campaign import Campaign
from runexpy.result import Result
from runexpy.runner import ParallelRunner, SimpleRunner
from runexpy.utils import DefaultParamsT, IterParamsT

from simulation.constants import DEFAULT_MAX_TIME, DEFAULT_NODES, DEFAULT_SEED, DEFAULT_LOGGING, DEFAULT_CLIENT_RATE, \
    DEFAULT_EXT, DEFAULT_ALPHA, DEFAULT_K, DEFAULT_QUEUE_CAPACITY, DEFAULT_N_KEYS, DEFAULT_JOINRATE, DEFAULT_CRASHRATE
from simulation.simulator import Simulator

CONF: IterParamsT = {
    "dht": [Simulator.KAD, Simulator.CHORD],
    "rate": [0.1, 0.2, 0.5, 1],
    "seed": list(range(420, 450)),
    "nkeys": [100, 2500],
    "crashrate": [0, 1, 10],
    "joinrate": [0, 1, 10],
}


def main():
    script = ["simulate"]

    campaign_dir = "campaigns/experiment"
    default_params: DefaultParamsT = {
        "seed": DEFAULT_SEED,
        "nodes": DEFAULT_NODES,
        "max-time": DEFAULT_MAX_TIME,
        "loglevel": DEFAULT_LOGGING,
        "rate": DEFAULT_CLIENT_RATE,
        "ext": DEFAULT_EXT,
        "alpha": DEFAULT_ALPHA,
        "k": DEFAULT_K,
        "capacity": DEFAULT_QUEUE_CAPACITY,
        "nkeys": DEFAULT_N_KEYS,
        "joinrate": DEFAULT_JOINRATE,
        "crashrate": DEFAULT_CRASHRATE,
        "dht": None,
    }

    campaign = Campaign.new(script, campaign_dir, default_params, overwrite=True)

    runner = ParallelRunner(48)
    # runner = SimpleRunner()
    campaign.run_missing_experiments(runner, CONF)


if __name__ == "__main__":
    main()
