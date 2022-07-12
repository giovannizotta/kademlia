import json
import os.path
from argparse import ArgumentParser, Namespace
from itertools import chain
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy
import numpy as np
from runexpy.campaign import Campaign
from runexpy.result import Result

from common.collector import DataCollector
from simulation.simulator import Simulator

DATA_FILE = "data.json"


def plot_healthy_nodes(dht_data: DataCollector) -> None:
    fig, ax = plt.subplots()
    ax.set_xlabel("Time")
    ax.set_ylabel("Healthy nodes")
    ax.set_title("Healthy nodes over time")
    ax.grid(True)

    joins = np.stack((list(dht_data.joined_time.values()), np.ones(len(dht_data.joined_time.values()))), axis=1)
    crash = np.stack((list(dht_data.crashed_time.values()), -np.ones(len(dht_data.crashed_time.values()))), axis=1)
    times = np.concatenate((joins, crash), axis=0)
    times = times[times[:, 0].argsort()]
    nodes = np.cumsum(times[:, 1])
    ax.plot(times[:, 0], nodes)

    plt.show()


def print_results(results: List[Tuple[Result, Dict[str, str]]]) -> None:
    for result, files in results:
        with open(files[DATA_FILE], "r") as f:
            data = json.load(f)
            dht_data = DataCollector.from_dict(data)
            plot_healthy_nodes(dht_data)


def main():
    campaign_dir = "campaigns/experiment"

    campaign = Campaign.load(campaign_dir)
    configs = {
        "dht": [Simulator.KAD],
    }
    results = campaign.get_results_for(configs)
    print_results(results)


if __name__ == "__main__":
    main()
