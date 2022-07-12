from runexpy.campaign import Campaign
from runexpy.result import Result
from runexpy.runner import ParallelRunner, SimpleRunner
from runexpy.utils import DefaultParamsT, IterParamsT

from simulation.run import DEFAULT_SEED, DEFAULT_NODES, DEFAULT_MAX_TIME, DEFAULT_LOGGING, DEFAULT_PLOT, DEFAULT_RATE, \
    DEFAULT_EXT, DEFAULT_ALPHA, DEFAULT_K, DEFAULT_QUEUE_CAPACITY, DEFAULT_N_KEYS, DEFAULT_JOINLAMBDA1, \
    DEFAULT_JOINLAMBDA2, DEFAULT_CRASHMU, DEFAULT_CRASHSIGMA
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
        "joinlambda1": DEFAULT_JOINLAMBDA1,
        "joinlambda2": DEFAULT_JOINLAMBDA2,
        "crashmu": DEFAULT_CRASHMU,
        "crashsigma": DEFAULT_CRASHSIGMA,
        "dht": None,
    }

    campaign = Campaign.new(script, campaign_dir, default_params, overwrite=True)

    configs: IterParamsT = {
        "dht": [Simulator.KAD, Simulator.CHORD],
        # "seed": [420],
        # "nodes": [50, 100, 2500],
        # "max-time": [10000, 100000, 250000],
        # "rate": [1, 2, 5, 10],
        # "nkeys": [10, 100, 2500],
        # churn,
    }

    # runner = ParallelRunner(10)
    runner = SimpleRunner()
    campaign.run_missing_experiments(runner, configs)
    results = campaign.get_results_for(configs)


if __name__ == "__main__":
    main()
