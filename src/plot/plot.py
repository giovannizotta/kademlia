import json
import os.path
from argparse import ArgumentParser, Namespace
from itertools import chain
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np

from common.collector import DataCollector
from simulation.simulator import Simulator

dhts = (Simulator.CHORD, Simulator.KAD)
rates = [0.1, 0.5, 0.2, 0.01]


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument(
        "-d", "--dht", choices=[Simulator.KAD, Simulator.CHORD], help="DHT to use"
    )
    ap.add_argument(
        "-e", "--ext", default="pdf", choices=["png", "pdf"], help="File extension"
    )
    ap.add_argument(
        "-a", "--arrivals", action="store_true", help="Compare arrivals or not"
    )
    ap.add_argument(
        "-i", "--input", type=str, default="res/data", help="Data directory"
    )
    ap.add_argument(
        "-o", "--output", type=str, default="res/plots", help="Output plot directory"
    )
    ap.add_argument("-r", "--rates", nargs="+", type=float, help="Rates", default=rates)
    ap.add_argument(
        "-s", "--singlerate", type=float, help="Rate for a single file", default=0.1
    )
    ap.add_argument("-n", "--nodes", type=int, required=True, help="Number of nodes")
    ap.add_argument(
        "-t", "--time", type=int, required=True, help="Duration of the simulations"
    )
    return ap.parse_args()


def get_heatmap(ax, delays, hops, max_delay, max_hops):
    hop_bins = [[] for _ in range(max_hops + 1)]
    for delay, hop in zip(delays, hops):
        hop_bins[hop].append(delay)
    im = []
    bins = np.linspace(0, int(max_delay), max(10, max_hops) + 1)
    for i in range(max_hops):
        hh, _ = np.histogram(hop_bins[i], bins=bins)
        im.append(hh)

    im_ratio = len(im) / len(im[0])
    im = np.array(im)
    im = ax.imshow(im, origin="lower")
    ticks = [f"{tick:.0f}" for tick in bins][:-1]
    ax.set_xticks(np.arange(len(ticks)), labels=ticks)
    # plt.show()
    cbar = ax.figure.colorbar(im, ax=ax, fraction=0.046 * im_ratio, location="right")
    cbar.ax.set_ylabel("Estimated density", rotation=-90, va="bottom")


def get_data(inputdir, n_nodes, time, rate=0):
    data = dict()
    max_hops = 0
    max_delay = 0
    max_time = 0
    for dht in dhts:
        pick = open(
            os.path.join(
                inputdir, f"{dht}_{n_nodes}_nodes_{time}_time_{rate:.01f}_rate.json"
            ),
            "r",
            encoding="utf8",
        )
        dct = json.load(pick)
        # print(dct)
        dht_data: DataCollector = DataCollector.from_dict(dct)
        data[dht] = dht_data
        delays, hops = zip(*data[dht].client_requests)
        t, _ = zip(*list(data[dht].queue_load.values())[0])
        max_time = max(max_time, max(t))
        max_hops = max(max_hops, max(hops))
        max_delay = max(max_delay, np.quantile(delays, 0.99))
    return data, max_hops, max_delay, max_time


def plot_comparison(data, max_hops, ext, nodes, outputdir):
    _, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 4))
    for dht in dhts:
        print(
            f"{dht} has had {data[dht].timed_out_requests} timeouts and "
            f"{len(data[dht].client_requests)} successful requests"
        )
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        ax1.hist(DHT_delays, bins=30, label=f"{dht}", alpha=0.6, density=True)
        ax2.hist(
            DHT_hops,
            bins=list(range(max_hops + 1)),
            label=f"{dht}",
            alpha=0.6,
            density=True,
        )

    ax1.set_title("Client request delays")
    ax2.set_title("Number of hops")
    ax1.set_xlabel("Client waiting time (ms)")
    ax2.set_xlabel("Number of hops")
    ax1.set_ylabel("Estimated density")
    ax2.set_ylabel("Estimated density")
    ax1.legend()
    ax2.legend()
    # fig.suptitle(f"Waiting time and number of hops, \
    #        {nodes} nodes", va="bottom", ha="center")
    plt.savefig(
        os.path.join(outputdir, f"delay_hops_comparison_{nodes}.{ext}"),
        format=ext,
        bbox_inches="tight",
    )


def plot_heatmap(data, max_hops, max_delay, ext, nodes, outputdir):
    _, (ax3, ax4) = plt.subplots(2, 1, figsize=(6, 8), constrained_layout=True)

    for ax, dht in zip((ax3, ax4), dhts):
        # ax3.scatter(DHT_delays, DHT_hops, label=f"{dht}", alpha=0.6)
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        assert DHT_hops.count(-1) == 0
        ax.set_title(f"{dht}")
        ax.set_ylabel("Number of hops")
        ax.set_xlabel("Client waiting time (ms)")
        get_heatmap(ax, DHT_delays, DHT_hops, max_delay, max_hops)

    # fig.suptitle(f"Correlation between delay and number of hops, \
    #         {nodes} nodes", va="bottom", ha="center")
    plt.savefig(
        os.path.join(outputdir, f"heatmap_{nodes}.{ext}"),
        format=ext,
        bbox_inches="tight",
    )


def get_avg_load(load: List[Tuple[float, int]], until: float = np.inf) -> float:
    previous_load = 0
    time = 0
    load_integral = 0
    for t, l in load:
        if t > until:
            break
        load_integral += previous_load * (t - time)
        previous_load = l
        time = t
    if time == 0:
        return 0
    return load_integral / time


def get_max_load(load: List[Tuple[float, int]], until: float = np.inf) -> float:
    return max((l for t, l in load if t < until), default=0)


def get_load_dist(data, dht, time):
    loads = []
    for _, load in data[dht].queue_load.items():
        l = get_max_load(load, until=time)
        # l = get_avg_load(load, until=time)
        loads.append(l)
    loads = np.array(loads)
    loads = loads[loads < np.quantile(loads, 0.95)]

    return loads


def plot_load_distrib(
    data: Dict[str, DataCollector], ext, nodes, max_time: float, outputdir: str
):
    _, axes = plt.subplots(2, 2, figsize=(6, 6), sharex=True, sharey=True)
    bound = 0
    for dht in dhts:
        bound = int(max(bound, max(get_load_dist(data, dht, np.inf))))

    for ax, time in zip(chain(*axes), np.linspace(1, max_time, num=5)[1:]):
        time = round(time)
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            ax.hist(
                loads, range=(0, bound), bins=25, alpha=0.6, label=dht, density=True
            )
            # ax.set_yscale('log')
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Estimated density")
        ax.set_title(f"Until {time} (s)")
        ax.legend()

    # fig.suptitle(f"Average node load distribution, \
    #         {nodes} nodes", va="bottom", ha="center")
    plt.savefig(
        os.path.join(outputdir, f"load_distr_{nodes}.{ext}"),
        format=ext,
        bbox_inches="tight",
    )


def plot_arrival_load_comparison(inputdir, ext, nodes, time, rates, outputdir):
    _, axes = plt.subplots(1, 4, figsize=(12, 3), constrained_layout=True)
    for ax, rate in zip(axes, rates):
        data, _, _, max_time = get_data(inputdir, nodes, time, rate=rate)
        time = round(max_time)
        timeouts = dict()
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            total_reqs = data[dht].timed_out_requests + len(data[dht].client_requests)
            timeouts[dht] = 100 * (data[dht].timed_out_requests / total_reqs)
            labl = f"{dht}"
            ax.hist(loads, bins=10, alpha=0.6, label=labl, density=True)
        title = [f"{dht}: {timeouts[dht]:4.1f}%" for dht in dhts]
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Estimated density")
        ax.set_title(f"Arrival rate: {1/rate:.1f}\nTO: {', '.join(title)}", fontsize=10)
        ax.legend()

    # fig.suptitle(f"Average load distribution with different arrival rates,
    #         {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"arrivals_load_{nodes}.{ext}"), format=ext)


def plot_arrival_delay_comparison(inputdir, ext, nodes, time, rates, outputdir):
    _, axes = plt.subplots(1, 4, figsize=(12, 3), constrained_layout=True)
    for ax, rate in zip(axes, rates):
        (
            data,
            _,
            _,
            _,
        ) = get_data(inputdir, nodes, time, rate=rate)
        timeouts = dict()
        for dht in dhts:
            DHT_delays, _ = zip(*data[dht].client_requests)
            total_reqs = data[dht].timed_out_requests + len(data[dht].client_requests)
            timeouts[dht] = 100 * (data[dht].timed_out_requests / total_reqs)
            labl = f"{dht}"
            ax.hist(DHT_delays, bins=30, label=labl, alpha=0.6, density=True)

        title = [f"{dht} {timeouts[dht]:4.1f}%" for dht in dhts]
        ax.set_title(f"Arrival rate: {1/rate:.1f}\nTO: {', '.join(title)}", fontsize=10)
        ax.set_xlabel("Client waiting time (ms)")
        ax.set_ylabel("Estimated density")
        ax.legend()
    # fig.suptitle(f"Average load distribution with different arrival rates,
    #         {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"arrivals_delay_{nodes}.{ext}"), format=ext)


def main():
    args = parse_args()
    if not args.arrivals:
        data, max_hops, max_delay, max_time = get_data(
            args.input, args.nodes, args.time, args.singlerate
        )
        print("Plotting delay comparison")
        plot_comparison(data, max_hops, args.ext, args.nodes, args.output)
        plt.clf()
        print("Plotting heatmap")
        plot_heatmap(data, max_hops, max_delay, args.ext, args.nodes, args.output)
        plt.clf()
        print("Plotting load distribution")
        plot_load_distrib(data, args.ext, args.nodes, max_time, args.output)
        plt.clf()
    else:
        print("Plotting arrival load comparison")
        plot_arrival_load_comparison(
            args.input, args.ext, args.nodes, args.time, args.rates, args.output
        )
        plt.clf()
        print("Plotting arrival delay comparison")
        plot_arrival_delay_comparison(
            args.input, args.ext, args.nodes, args.time, args.rates, args.output
        )
        plt.clf()


if __name__ == "__main__":
    main()
