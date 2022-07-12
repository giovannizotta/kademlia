from runexpy.campaign import Campaign
from runexpy.result import Result
from runexpy.runner import ParallelRunner, SimpleRunner
from runexpy.utils import DefaultParamsT, IterParamsT

from simulation.run import DEFAULT_SEED, DEFAULT_NODES, DEFAULT_MAX_TIME, DEFAULT_LOGGING, DEFAULT_PLOT, DEFAULT_RATE, \
    DEFAULT_EXT, DEFAULT_ALPHA, DEFAULT_K, DEFAULT_QUEUE_CAPACITY, DEFAULT_N_KEYS
from simulation.simulator import Simulator


def main():
    script = ["simulate"]

    campaign_dir = "campaigns/experiment"
    default_params: DefaultParamsT = {
        "seed": DEFAULT_SEED,
        "nodes": DEFAULT_NODES,
        "max-time": DEFAULT_MAX_TIME,
        "loglevel": DEFAULT_LOGGING,
        "rate": DEFAULT_RATE,
        "ext": DEFAULT_EXT,
        "alpha": DEFAULT_ALPHA,
        "k": DEFAULT_K,
        "capacity": DEFAULT_QUEUE_CAPACITY,
        "nkeys": DEFAULT_N_KEYS,
        "dht": None,
    }

    campaign = Campaign.new(script, campaign_dir, default_params, overwrite=True)

    configs: IterParamsT = {
        "dht": [Simulator.KAD, Simulator.CHORD],
        # "seed": [420],
        # "n": [100, 1000, 2500],
        # "max_time": [10000, 100000, 1000000],
        # "rate": [3, 5, 10],
        # "alpha": [3, 5],
        # "k": [5, 15],
        # "nkeys": [100, 1000, 2500],
        # "seed": [420],
        # "n": [100, 1000, 2500],
        # "max_time": [10000, 100000, 1000000],
        # "rate": [3, 5, 10],
        # "alpha": [3, 5],
        # "k": [5, 15],
        # "nkeys": [100, 1000, 2500],
    }

    # runner = ParallelRunner(10)
    runner = SimpleRunner()
    campaign.run_missing_experiments(runner, configs)
    results = campaign.get_results_for(configs)


if __name__ == "__main__":
    main()
