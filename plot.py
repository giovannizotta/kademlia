import json
import matplotlib.pyplot as plt
import numpy as np
import os.path
from common.node import DataCollector
from common.utils import *
from common.simulator import Simulator
from argparse import ArgumentParser, Namespace
from itertools import chain
dhts = (Simulator.CHORD, Simulator.KAD)


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument("-d", "--dht",
                    choices=[Simulator.KAD, Simulator.CHORD], help="DHT to use")
    ap.add_argument("-e", "--ext", default="pdf",
                    choices=["png", "pdf"], help="File extension")
    ap.add_argument("-a", "--arrivals", action="store_true",
                    help="Compare arrivals or not")
    ap.add_argument("-i", "--input", type=str, default="res/data",
                    help="Data directory")
    ap.add_argument("-o", "--output", type=str, default="res/plots",
                    help="Output plot directory")
    return ap.parse_args()


def get_heatmap(ax, delays, hops, max_delay, max_hops):
    hop_bins = [[] for _ in range(max_hops+1)]
    for delay, hop in zip(delays, hops):
        hop_bins[hop].append(delay)
    im = []
    bins=np.linspace(0, int(max_delay), max(10, max_hops) + 1)
    for i in range(max_hops):
        hh, _ = np.histogram(hop_bins[i], bins=bins)
        im.append(hh)

    im_ratio = len(im) / len(im[0])
    im = np.array(im)
    im = ax.imshow(im, origin="lower")
    ticks = [f"{tick:.1f}" for tick in bins][:-1]
    ax.set_xticks(np.arange(len(ticks)), labels=ticks)
    # plt.show()
    cbar = ax.figure.colorbar(
        im, ax=ax, fraction=0.046*im_ratio, location="right")
    cbar.ax.set_ylabel("Density", rotation=-90, va="bottom")


def get_data(inputdir, rate=0):
    data = dict()
    max_hops = 0
    max_delay = 0
    max_time = 0
    nodes = 0
    for dht in dhts:
        if rate == 0:
            pick = open(os.path.join(inputdir, f"{dht}.json"), "r", encoding='utf8')
        else:
            pick = open(os.path.join(inputdir, f"{dht}_{rate}.json"), "r", encoding='utf8')
        dct = json.load(pick)
        # print(dct)
        dht_data: DataCollector = DataCollector.from_dict(dct)
        data[dht] = dht_data
        delays, hops = zip(*data[dht].client_requests)
        t, _ = zip(*list(data[dht].queue_load.values())[0])
        max_time = max(max_time, max(t))
        max_hops = max(max_hops, max(hops))
        max_delay = max(max_delay, np.quantile(delays, 0.99))
        nodes = len(data[dht].queue_load)
    return data, max_hops, max_delay, max_time, nodes


def plot_comparison(data, max_hops, ext, nodes, outputdir):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 10), constrained_layout=True)
    for dht in dhts:
        print(
            f"{dht} has had {data[dht].timed_out_requests} timeouts and {len(data[dht].client_requests)} successful requests")
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        ax1.hist(DHT_delays, bins=30,
                 label=f"{dht}", alpha=0.6, density=True)
        ax2.hist(DHT_hops, bins=list(range(max_hops + 1)),
                 label=f"{dht}", alpha=0.6, density=True)

    ax1.set_title("Client request delays")
    ax2.set_title("Number of hops needed to fulfill a client request")
    ax1.set_xlabel("Client waiting time (s)")
    ax2.set_xlabel("Number of hops")
    ax1.set_ylabel("Estimated probability")
    ax2.set_ylabel("Estimated probability")
    ax1.legend()
    ax2.legend()
    #fig.suptitle(f"Waiting time and number of hops, {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"delay_hops_comparison_{nodes}.{ext}"),
                format=ext, bbox_inches="tight")


def plot_heatmap(data, max_hops, max_delay, ext, nodes, outputdir):
    fig, (ax3, ax4) = plt.subplots(2, 1, figsize=(6, 8), constrained_layout=True)

    for ax, dht in zip((ax3, ax4), dhts):
        #ax3.scatter(DHT_delays, DHT_hops, label=f"{dht}", alpha=0.6)
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        assert DHT_hops.count(-1) == 0
        ax.set_title(f"{dht}")
        ax.set_ylabel("Number of hops")
        ax.set_xlabel("Client waiting time (s)")
        get_heatmap(ax, DHT_delays, DHT_hops, max_delay, max_hops)

    #fig.suptitle(f"Correlation between delay and number of hops, {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"heatmap_{nodes}.{ext}"), format=ext, bbox_inches="tight")

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


def get_load_dist(data, dht, time):
    loads = []
    for _, load in data[dht].queue_load.items():
        avg_load= get_avg_load(load, until=time)
        loads.append(avg_load)
    loads = np.array(loads)
    loads = loads[loads < np.quantile(loads, 0.95)]

    return loads

def plot_load_distrib(data: Dict[str, DataCollector], ext, nodes, max_time: float, outputdir: str):
    fig, axes = plt.subplots(4, 2, figsize=(10, 16), sharex=True, sharey=True)
    for ax, time in zip(chain(*axes), np.linspace(1, max_time, num=9)[1:]):
        time = round(time)
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            # hh, bb = np.histogram(loads, bins=20)
            # print(hh[-10:], bb[-10:])
            ax.hist(loads, bins=10, alpha=0.6, label=dht, density=True)
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Density")
        ax.set_title(f"Until {time} (s)")
        ax.legend()

    #fig.suptitle(f"Average node load distribution, {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"load_distr_{nodes}.{ext}"), format=ext)

def plot_arrival_comparison(inputdir, ext, outputdir):
    fig, axes = plt.subplots(2, 2, figsize=(10, 10), constrained_layout=True)
    nodes = 0
    for ax, rate in zip(chain(*axes), (0.01, 0.02, 0.05, 0.1)):
        data, _, _, max_time, nodes = get_data(inputdir, rate=rate)
        time = round(max_time)
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            total_reqs = data[dht].timed_out_requests + len(data[dht].client_requests)
            labl = f"{dht}, timeouts: {data[dht].timed_out_requests} / {total_reqs}"
            ax.hist(loads, bins=10, alpha=0.6, label=labl, density=True)
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Density")
        ax.set_title(f"Arrival rate: {rate}")
        ax.legend()

    #fig.suptitle(f"Average load distribution with different arrival rates, {nodes} nodes", va="bottom", ha="center")
    plt.savefig(os.path.join(outputdir, f"arrivals_{nodes}.{ext}"), format=ext)

def main():
    args = parse_args()
    if not args.arrivals:
        data, max_hops, max_delay, max_time, nodes = get_data(args.input)
        plot_comparison(data, max_hops, args.ext, nodes, args.output)
        plt.clf()
        plot_heatmap(data, max_hops, max_delay, args.ext, nodes, args.output)
        plt.clf()
        plot_load_distrib(data, args.ext, nodes, max_time, args.output)
        plt.clf()
    else:
        plot_arrival_comparison(args.input, args.ext, args.output)
            


if __name__ == "__main__":
    main()
