from runexpy.campaign import Campaign
from runexpy.runner import ParallelRunner
from runexpy.utils import DefaultParamsT

from simulation.constants import DEFAULT_MAX_TIME, DEFAULT_NODES, DEFAULT_SEED, DEFAULT_LOGGING, DEFAULT_CLIENT_RATE, \
    DEFAULT_EXT, DEFAULT_ALPHA, DEFAULT_K, DEFAULT_QUEUE_CAPACITY, DEFAULT_N_KEYS, DEFAULT_JOINRATE, DEFAULT_CRASHRATE
from simulation.simulator import Simulator

normal_conf = {
    "dht": [Simulator.KAD, Simulator.CHORD],
    "rate": [0.1, 0.5, 1],
    "seed": list(range(420, 430)),
    "nkeys": [1000, 10000],
    "crashrate": [0.01, 0.1],
    "joinrate": [0.1, 1],
}

high_churn_conf = {
    "dht": [Simulator.KAD, Simulator.CHORD],
    "rate": [0.1, 0.5, 1],
    "seed": list(range(420, 430)),
    "nkeys": [1000, 10000],
    "crashrate": [1],
    "joinrate": [10],
}

no_churn_conf = {
    "dht": [Simulator.KAD, Simulator.CHORD],
    "rate": [0.1, 0.5, 1],
    "seed": list(range(420, 430)),
    "nkeys": [1000, 10000],
    "crashrate": [0],
    "joinrate": [0],
}

high_load_conf = {
    "dht": [Simulator.KAD, Simulator.CHORD],
    "rate": [10],
    "seed": list(range(420, 430)),
    "nkeys": [1000, 10000],
    "crashrate": [0, 0.1],
    "joinrate": [0, 1],
}

CONF = [normal_conf, high_churn_conf, no_churn_conf]


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
    print(f"Executing {len(list(campaign.list_param_combinations(CONF)))} experiments")

    runner = ParallelRunner(100)
    campaign.run_missing_experiments(runner, CONF)


if __name__ == "__main__":
    main()
