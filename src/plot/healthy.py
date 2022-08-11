import json
import os.path
from argparse import ArgumentParser, Namespace
from itertools import chain
from typing import Dict, List, Tuple

import altair as alt
import matplotlib.pyplot as plt
import numpy
import numpy as np
import pandas as pd
from runexpy.campaign import Campaign
from runexpy.result import Result
from runexpy.utils import IterParamsT

from common.collector import DataCollector
from plot.data import get_join_time, get_crash_time
from simulation.simulator import Simulator

DATA_FILE = "data.json"


def get_healthy_chart(conf: IterParamsT) -> Tuple[alt.Chart, alt.Chart]:
    join_time_df = get_join_time(conf)
    crash_time_df = get_crash_time(conf)
    maxtime = join_time_df["max-time"].iloc[0]

    slot_width = maxtime / 100
    join_time_df["increment"] = 1
    crash_time_df["increment"] = -1

    df = pd.concat([join_time_df, crash_time_df], ignore_index=True).sort_values(by="time", ignore_index=True)
    df["count"] = df["increment"].cumsum()

    df["time_slot"] = df["time"].apply(lambda x: x // slot_width).astype(int)
    df = df.groupby(["time_slot", "seed"]).agg(
        time_slot_mean=("count", "mean"),
    ).reset_index()

    df_ci = df.groupby(["time_slot"]).agg(
        mean=("time_slot_mean", "mean"),
        sem=("time_slot_mean", "sem"),
    ).reset_index()
    df_ci["dht"] = conf["dht"]

    df_ci["ci95_hi"] = df_ci["mean"] + 1.96 * df_ci["sem"]
    df_ci["ci95_lo"] = df_ci["mean"] - 1.96 * df_ci["sem"]

    print(df_ci["ci95_hi"], df_ci["ci95_lo"])

    line = alt.Chart(df_ci).mark_line().encode(
        x=alt.X('time_slot', axis=alt.Axis(title="Time")),
        y=alt.Y('mean', title="Mean"),
        color=alt.Color('dht', legend=alt.Legend(title="DHT")),
        tooltip=['mean', 'ci95_lo', 'ci95_hi'],
    )
    ci = alt.Chart(df_ci).mark_area(opacity=0.2).encode(
        x=alt.X('time_slot', axis=alt.Axis(title="Time")),
        y=alt.Y('ci95_hi', title="95% CI"),
        y2=alt.Y('ci95_lo', title="95% CI"),
        color=alt.Color('dht', legend=alt.Legend(title="DHT")),
        tooltip=['mean', 'ci95_hi', 'ci95_lo'],
    )
    return line, ci



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
